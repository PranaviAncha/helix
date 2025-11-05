package org.apache.helix.model;

import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Test to demonstrate the evacuation issue when HELIX_ENABLED=false
 * and EVACUATE operation is set - getInstanceOperation() returns DISABLE instead of EVACUATE.
 */
public class TestInstanceConfigEvacuateWithHelixDisabled {

  private void printState(String label, InstanceConfig config) {
    System.out.println("\n" + label);
    System.out.println("  HELIX_ENABLED: " + config.getInstanceEnabled());
    
    List<String> opsList = config.getRecord().getListField(
        InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name());
    System.out.println("  HELIX_INSTANCE_OPERATIONS (storage): " + 
        (opsList == null ? "[]" : opsList));
    
    try {
      // Use reflection to call private getActiveInstanceOperation()
      Method getActiveOp = InstanceConfig.class.getDeclaredMethod("getActiveInstanceOperation");
      getActiveOp.setAccessible(true);
      InstanceConfig.InstanceOperation activeOp = 
          (InstanceConfig.InstanceOperation) getActiveOp.invoke(config);
      System.out.println("  getActiveInstanceOperation(): " + activeOp.getOperation() + 
          " (SOURCE=" + activeOp.getSource() + ")");
    } catch (Exception e) {
      System.out.println("  getActiveInstanceOperation(): [unable to access]");
    }
    
    InstanceConfig.InstanceOperation returnedOp = config.getInstanceOperation();
    System.out.println("  getInstanceOperation(): " + returnedOp.getOperation() + 
        " (SOURCE=" + returnedOp.getSource() + ")");
  }

  @Test
  public void testEvacuateWithHelixDisabledCompleteTimeline() throws InterruptedException {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("TEST: Evacuate with HELIX_ENABLED=false");
    System.out.println("=".repeat(80));
    
    ZNRecord record = new ZNRecord("testInstance");
    InstanceConfig instanceConfig = new InstanceConfig(record);
    
    // T-1: Enable instance
    System.out.println("\n>>> T-1: Instance is enabled <<<");
    InstanceConfig.InstanceOperation enableOp = 
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.ENABLE)
            .setSource(InstanceConstants.InstanceOperationSource.ADMIN)
            .build();
    instanceConfig.setInstanceOperation(enableOp);
    
    printState("STATE AFTER ENABLE:", instanceConfig);
    
    Assert.assertTrue(instanceConfig.getInstanceEnabled());
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.ENABLE);
    
    // Sleep to separate timestamps
    System.out.println("\n[Sleeping 5 seconds to separate timestamps...]");
    Thread.sleep(5000);
    
    // T0: Disable instance
    System.out.println("\n" + "=".repeat(80));
    System.out.println("\n>>> T0: User disables instance <<<");
    instanceConfig.getRecord().setSimpleField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "false");
    
    InstanceConfig.InstanceOperation disableOp = 
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.DISABLE)
            .setSource(InstanceConstants.InstanceOperationSource.USER)
            .build();
    instanceConfig.setInstanceOperation(disableOp);
    
    printState("STATE AFTER DISABLE:", instanceConfig);
    
    Assert.assertFalse(instanceConfig.getInstanceEnabled());
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(),
        InstanceConstants.InstanceOperation.DISABLE);
    
    // Sleep to separate timestamps
    System.out.println("\n[Sleeping 5 seconds to separate timestamps...]");
    Thread.sleep(5000);
    
    // T1: Try to evacuate
    System.out.println("\n" + "=".repeat(80));
    System.out.println("\n>>> T1: ACM sets EVACUATE operation <<<");
    InstanceConfig.InstanceOperation evacuateOp = 
        new InstanceConfig.InstanceOperation.Builder()
            .setOperation(InstanceConstants.InstanceOperation.EVACUATE)
            .setSource(InstanceConstants.InstanceOperationSource.AUTOMATION)
            .build();
    instanceConfig.setInstanceOperation(evacuateOp);
    
    printState("STATE AFTER EVACUATE:", instanceConfig);
    
    String operationsList = instanceConfig.getRecord().getListField(
        InstanceConfig.InstanceConfigProperty.HELIX_INSTANCE_OPERATIONS.name()).toString();
    Assert.assertTrue(operationsList.contains("EVACUATE"), "Storage should contain EVACUATE");
    Assert.assertTrue(operationsList.contains("DISABLE"), "Storage should contain DISABLE");
    
    // T2: Demonstrate the issue
    System.out.println("\n" + "=".repeat(80));
    System.out.println("\n>>> T2: THE ISSUE <<<");
    System.out.println("  Storage has EVACUATE, but getInstanceOperation() returns DISABLE!");
    System.out.println("  Reason: Override triggered at line 708-724 in InstanceConfig.java");
    System.out.println("          HELIX_ENABLED=false + activeOp=EVACUATE → returns DISABLE");
    
    InstanceConfig.InstanceOperation returnedOp = instanceConfig.getInstanceOperation();
    Assert.assertEquals(returnedOp.getOperation(), InstanceConstants.InstanceOperation.DISABLE,
        "Override returns DISABLE instead of EVACUATE");
    
    // T3: Check isEvacuateFinished logic
    System.out.println("\n" + "=".repeat(80));
    System.out.println("\n>>> T3: isEvacuateFinished() check <<<");
    boolean isEvacuateFinished = 
        (instanceConfig.getInstanceOperation().getOperation() == 
         InstanceConstants.InstanceOperation.EVACUATE);
    
    System.out.println("  ZKHelixAdmin.isEvacuateFinished() logic (line 464-466):");
    System.out.println("    if (config.getInstanceOperation().getOperation() != EVACUATE)");
    System.out.println("        return false;");
    System.out.println("\n  getInstanceOperation().getOperation() = " + 
        instanceConfig.getInstanceOperation().getOperation());
    System.out.println("  Result: " + isEvacuateFinished);
    System.out.println("  → Evacuation cannot complete, ACM keeps polling");
    
    Assert.assertFalse(isEvacuateFinished, "isEvacuateFinished() returns false");
    
    // Fix
    System.out.println("\n" + "=".repeat(80));
    System.out.println("\n>>> FIX: Set HELIX_ENABLED=true <<<");
    instanceConfig.getRecord().setSimpleField(
        InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true");
    
    printState("STATE AFTER FIX:", instanceConfig);
    
    boolean isEvacuateFinishedAfterFix = 
        (instanceConfig.getInstanceOperation().getOperation() == 
         InstanceConstants.InstanceOperation.EVACUATE);
    System.out.println("\n  isEvacuateFinished() check: " + isEvacuateFinishedAfterFix);
    System.out.println("  → Evacuation can proceed ✅");
    
    Assert.assertEquals(instanceConfig.getInstanceOperation().getOperation(), 
        InstanceConstants.InstanceOperation.EVACUATE);
    Assert.assertTrue(isEvacuateFinishedAfterFix);
    
    System.out.println("\n" + "=".repeat(80) + "\n");
  }
}
