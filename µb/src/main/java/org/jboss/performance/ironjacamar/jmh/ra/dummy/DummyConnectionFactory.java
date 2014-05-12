package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import java.io.Serializable;

import javax.resource.Referenceable;
import javax.resource.ResourceException;

public interface DummyConnectionFactory extends Serializable, Referenceable {

   public DummyConnection getConnection() throws ResourceException;

}
