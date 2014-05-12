package org.jboss.performance.ironjacamar.jmh.ra.dummy;

import javax.resource.cci.ResourceAdapterMetaData;

public class DummyRaMetaData implements ResourceAdapterMetaData {

   public DummyRaMetaData() {
   }

   /**
    * Gets the version of the resource adapter.
    */
   @Override
   public String getAdapterVersion() {
      return null; //TODO
   }

   /**
    * Gets the name of the vendor that has provided the resource adapter.
    */
   @Override
   public String getAdapterVendorName() {
      return null; //TODO
   }

   /**
    * Gets a tool displayable name of the resource adapter.
    */
   @Override
   public String getAdapterName() {
      return null; //TODO
   }

   /**
    * Gets a tool displayable short description of the resource adapter.
    */
   @Override
   public String getAdapterShortDescription() {
      return null; //TODO
   }

   /**
    * Returns a string representation of the version
    */
   @Override
   public String getSpecVersion() {
      return null; //TODO
   }

   /**
    * Returns an array of fully-qualified names of InteractionSpec
    */
   @Override
   public String[] getInteractionSpecsSupported() {
      return null; //TODO
   }

   /**
    * Returns true if the implementation class for the Interaction
    */
   @Override
   public boolean supportsExecuteWithInputAndOutputRecord()  {
      return false; //TODO
   }

   /**
    * Returns true if the implementation class for the Interaction
    */
   @Override
   public boolean supportsExecuteWithInputRecordOnly() {
      return false; //TODO
   }

   /**
    * Returns true if the resource adapter implements the LocalTransaction
    */
   @Override
   public boolean supportsLocalTransactionDemarcation() {
      return false; //TODO
   }

}
