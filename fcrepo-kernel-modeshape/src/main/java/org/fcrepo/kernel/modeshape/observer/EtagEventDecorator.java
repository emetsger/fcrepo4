/*
 * Licensed to DuraSpace under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 *
 * DuraSpace licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.kernel.modeshape.observer;

import org.fcrepo.kernel.api.observer.EventType;
import org.fcrepo.kernel.api.observer.FedoraEvent;
import org.fcrepo.kernel.modeshape.FedoraResourceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.jcr.RepositoryException;
import javax.jcr.Session;
import java.util.function.Predicate;

/**
 * @author Elliot Metsger (emetsger@jhu.edu)
 */
@Component
public class EtagEventDecorator implements FedoraEventDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(EtagEventDecorator.class);

    @Override
    public Predicate<FedoraEvent> decorates() {
        return fedoraEvent -> (fedoraEvent.getTypes().contains(EventType.RESOURCE_CREATION) || fedoraEvent.getTypes()
                .contains(EventType.RESOURCE_MODIFICATION)) &&
                (fedoraEvent.getPath() != null && fedoraEvent.getPath().trim().length() > 0);
    }

    @Override
    public void accept(final Session session, final FedoraEvent fedoraEvent) {
        try {
            fedoraEvent.getInfo()
                    .put("etag", new FedoraResourceImpl(session.getNode(fedoraEvent.getPath())).getEtagValue());
        } catch (RepositoryException e) {
            LOG.warn("Error decorating event {} with etag: {}", fedoraEvent.getEventID(), e.getMessage());
        }
    }

}
