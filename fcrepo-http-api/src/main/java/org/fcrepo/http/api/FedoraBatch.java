/**
 * Copyright 2014 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.http.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static javax.ws.rs.core.MediaType.MULTIPART_FORM_DATA;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.noContent;
import static javax.ws.rs.core.Response.notAcceptable;
import static javax.ws.rs.core.Response.ok;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.Status;
import static org.apache.jena.riot.WebContent.contentTypeSPARQLUpdate;
import static org.modeshape.jcr.api.JcrConstants.JCR_CONTENT;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.io.IOUtils;
import org.fcrepo.kernel.Datastream;
import org.fcrepo.kernel.FedoraBinary;
import org.fcrepo.kernel.FedoraObject;
import org.fcrepo.kernel.FedoraResource;
import org.fcrepo.kernel.exception.InvalidChecksumException;
import org.fcrepo.kernel.exception.RepositoryRuntimeException;
import org.fcrepo.kernel.utils.ContentDigest;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.ContentDisposition;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.slf4j.Logger;
import org.springframework.context.annotation.Scope;

import com.codahale.metrics.annotation.Timed;

/**
 * Controller for manipulating binary streams in larger batches
 * by using multipart requests and responses
 *
 * @author cbeer
 */
@Scope("request")
@Path("/{path: .*}/fcr:batch")
public class FedoraBatch extends ContentExposingResource {

    public static final String ATTACHMENT = "attachment";
    public static final String INLINE = "inline";
    public static final String DELETE = "delete";
    public static final String FORM_DATA_DELETE_PART_NAME = "delete[]";
    @Inject
    protected Session session;

    private static final Logger LOGGER = getLogger(FedoraBatch.class);

    @PathParam("path") protected String externalPath;

    /**
     * Run these actions after initializing this resource
     */
    @PostConstruct
    public void postConstruct() {
        setUpJMSBaseURIs(uriInfo);
    }

    /**
     * Default JAX-RS entry point
     */
    public FedoraBatch() {
        super();
    }

    /**
     * Create a new FedoraNodes instance for a given path
     * @param externalPath
     */
    @VisibleForTesting
    public FedoraBatch(final String externalPath) {
        this.externalPath = externalPath;
    }

    /**
     * Apply batch modifications relative to the node.
     *
     * This endpoint supports two types of multipart requests:
     *  - mixed (preferred)
     *  - form-data (fallback for "dumb" clients)
     *
     *  The name-part of the multipart request is relative to the node
     *  this operation was called on.
     *
     *  multipart/mixed:
     *
     *  mixed mode supports three content-disposition types:
     *  - inline
     *      Add or update an objects triples
     *  - attachment
     *      Add or update binary content
     *  - delete
     *      Delete an object
     *
     *  multipart/form-data:
     *
     *  form-data is a fallback for dumb clients (e.g. HttpClient, curl, etc).
     *  Instead of using the Content-Disposition to determine what operation
     *  to perform, form-data uses heuristics to figure out what to do.
     *
     *  - if the entity has a filename, always treat it as binary content
     *  - if the content is RDF or SPARQL-Update, add or update triples
     *  - if the entity has the name "delete[]", the body is a single path
     *  to delete.
     *  - otherwise, treat the entity as binary content.
     *
     *
     * @param multipart
     * @return response
     * @throws RepositoryException
     * @throws IOException
     * @throws InvalidChecksumException
     */
    @POST
    @Timed
    public Response batchModify(final MultiPart multipart)
        throws InvalidChecksumException, IOException, URISyntaxException {

        final String path = toPath(translator(), externalPath);

        final Set<FedoraResource> resourcesChanged = new HashSet<>();

        // iterate through the multipart entities
        for (final BodyPart part : multipart.getBodyParts()) {
            final ContentDisposition contentDisposition = part.getContentDisposition();


            // a relative path (probably.)

            final String contentDispositionType = contentDisposition.getType();

            final String partName = contentDisposition.getParameters().get("name");

            final String contentTypeString = getSimpleContentType(part.getMediaType()).toString();

            LOGGER.trace("Processing {} part {} with media type {}",
                    contentDispositionType, partName, contentTypeString);

            final String realContentDisposition;

            // we need to apply some heuristics for "dumb" clients that
            // can only send form-data content
            if (contentDispositionType.equals("form-data")) {

                if (contentDisposition.getFileName() != null) {
                    realContentDisposition = ATTACHMENT;
                } else if (contentTypeString.equals(contentTypeSPARQLUpdate)
                        || (isRdfContentType(contentTypeString) && !contentTypeString.equals(TEXT_PLAIN))) {
                    realContentDisposition = INLINE;
                } else if (partName.equals(FORM_DATA_DELETE_PART_NAME)) {
                    realContentDisposition = DELETE;
                } else {
                    realContentDisposition = ATTACHMENT;
                }

                LOGGER.trace("Converted form-data to content disposition {}", realContentDisposition);
            } else {
                realContentDisposition = contentDispositionType;
            }

            // convert the entity to an InputStream
            final Object entityBody = part.getEntity();

            final InputStream src;
            if (entityBody instanceof BodyPartEntity) {
                final BodyPartEntity entity =
                        (BodyPartEntity) part.getEntity();
                src = entity.getInputStream();
            } else if (entityBody instanceof InputStream) {
                src = (InputStream) entityBody;
            } else {
                LOGGER.debug("Got unknown multipart entity for {}; ignoring it", partName);
                src = IOUtils.toInputStream("");
            }

            // convert the entity name to a node path
            final String pathName;

            if (partName.equals(FORM_DATA_DELETE_PART_NAME)) {
                pathName = IOUtils.toString(src);
            } else {
                pathName = partName;
            }

            final String absPath;

            if (pathName.startsWith("/")) {
                absPath = pathName;
            } else {
                absPath = path + "/" + pathName;
            }
            final String objPath = translator().asString(translator().toDomain(absPath));

            final FedoraResource resource;

            if (nodeService.exists(session, objPath)) {
                resource = getResourceFromPath(objPath);
            } else if ((isRdfContentType(contentTypeString) && !contentTypeString.equals(TEXT_PLAIN)) ||
                    contentTypeString.equals(contentTypeSPARQLUpdate)) {
                resource = objectService.findOrCreateObject(session, objPath);
            } else {
                resource = binaryService.findOrCreateBinary(session, objPath);
            }

            final String checksum = contentDisposition.getParameters().get("checksum");

            switch (realContentDisposition) {
                case INLINE:
                case ATTACHMENT:

                    if ((resource instanceof FedoraObject || resource instanceof Datastream)
                            && isRdfContentType(contentTypeString)) {
                        replaceResourceWithStream(resource, src, part.getMediaType());
                    } else if (resource instanceof FedoraBinary) {
                        replaceResourceBinaryWithStream((FedoraBinary) resource,
                                src,
                                contentDisposition,
                                part.getMediaType().toString(),
                                checksum);
                    } else if (contentTypeString.equals(contentTypeSPARQLUpdate)) {
                        patchResourcewithSparql(resource, IOUtils.toString(src));
                    } else {
                        throw new WebApplicationException(notAcceptable(null)
                                .entity("Invalid Content Type " + contentTypeString).build());
                    }

                    resourcesChanged.add(resource);

                    break;

                case DELETE:
                    resource.delete();
                    break;

                default:
                    return status(Status.BAD_REQUEST)
                            .entity("Unknown Content-Disposition: " + realContentDisposition).build();
            }
        }

        try {
            session.save();
            versionService.nodeUpdated(session, path);
            for (final FedoraResource resource : resourcesChanged) {
                versionService.nodeUpdated(resource.getNode());
            }
        } catch (final RepositoryException e) {
            throw new RepositoryRuntimeException(e);
        }

        return noContent().build();

    }

    /**
     * Retrieve multiple datastream bitstreams in a single request as a
     * multipart/mixed response.
     *
     * @param requestedChildren
     * @param request
     * @return response
     * @throws RepositoryException
     * @throws NoSuchAlgorithmException
     */
    @GET
    @Produces("multipart/mixed")
    @Timed
    public Response getBinaryContents(@QueryParam("child") final List<String> requestedChildren,
        @Context final Request request) throws RepositoryException, NoSuchAlgorithmException {

        final List<FedoraBinary> binaries = new ArrayList<>();
        // TODO: wrap some of this JCR logic in an fcrepo abstraction;

        final Node node = resource().getNode();

        Date date = new Date();

        final MessageDigest digest = MessageDigest.getInstance("SHA-1");

        final NodeIterator ni;

        if (requestedChildren.isEmpty()) {
            ni = node.getNodes();
        } else {
            ni = node.getNodes(requestedChildren
                            .toArray(new String[requestedChildren.size()]));
        }

        // complain if no children found
        if ( ni.getSize() == 0 ) {
            return status(Status.BAD_REQUEST).build();
        }

        // transform the nodes into datastreams, and calculate cache header
        // data
        while (ni.hasNext()) {

            final Node dsNode = ni.nextNode();
            final FedoraBinary binary = binaryService.asBinary(dsNode.getNode(JCR_CONTENT));

            digest.update(binary.getContentDigest().toString().getBytes(
                    UTF_8));

            if (binary.getLastModifiedDate().after(date)) {
                date = binary.getLastModifiedDate();
            }

            binaries.add(binary);
        }

        final URI digestURI = ContentDigest.asURI(digest.getAlgorithm(), digest.digest());
        final EntityTag etag = new EntityTag(digestURI.toString());

        final Date roundedDate = new Date();
        roundedDate.setTime(date.getTime() - date.getTime() % 1000);

        ResponseBuilder builder =
                request.evaluatePreconditions(roundedDate, etag);

        final CacheControl cc = new CacheControl();
        cc.setMaxAge(0);
        cc.setMustRevalidate(true);

        if (builder == null) {
            final MultiPart multipart = new MultiPart();

            for (final FedoraBinary binary : binaries) {
                final BodyPart bodyPart =
                        new BodyPart(binary.getContent(),
                                MediaType.valueOf(binary.getMimeType()));
                bodyPart.setContentDisposition(
                        ContentDisposition.type(ATTACHMENT)
                                .fileName(translator().reverse().convert(binary.getNode()).toString())
                                .creationDate(binary.getCreatedDate())
                                .modificationDate(binary.getLastModifiedDate())
                                .size(binary.getContentSize())
                                .build());
                multipart.bodyPart(bodyPart);
            }

            builder = ok(multipart, MULTIPART_FORM_DATA);
        }

        return builder.cacheControl(cc).lastModified(date).tag(etag)
                .build();

    }

    @Override
    protected Session session() {
        return session;
    }

    @Override
    void addResourceHttpHeaders(final FedoraResource resource) {

    }

    @Override
    String externalPath() {
        return externalPath;
    }
}
