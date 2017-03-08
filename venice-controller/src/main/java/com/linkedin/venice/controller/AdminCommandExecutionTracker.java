package com.linkedin.venice.controller;

import com.linkedin.venice.controllerapi.AdminCommandExecution;
import com.linkedin.venice.controllerapi.ControllerClient;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;


/**
 * This class is used to track the execution of the async admin command. Async admin command is a kind of admin
 * command which is send to a parent controller and propagated through a Kafka admin topic. Eventually command would be
 * executed by the controller in each PROD fabric.
 * <p>
 * The context of command execution would be stored in this tracker and expired in case it lives longer than TTL. It
 * also provides a way to check the latest execution status of given command by sending requests to remote fabric. But
 * the checking logic is defined in a closure when the command was created.
 * <p>
 * This class is Thread-safe.
 */
public class AdminCommandExecutionTracker {
  public static final int DEFAULT_TTL_HOUR = 24;
  private static final Logger logger = Logger.getLogger(AdminCommandExecutionTracker.class);

  private final String cluster;
  private final int executionTTLHour;
  private final LinkedHashMap<Long, AdminCommandExecution> idToExecutionMap;
  private final AtomicLong idGenerator;
  private final Map<String, ControllerClient> fabricToControllerClientsMap;
  private final ExecutionIdAccessor executionIdAccessoridAccessor;

  public AdminCommandExecutionTracker(String cluster, ExecutionIdAccessor executionIdAccessoridAccessor,
      Map<String, ControllerClient> fabricToControllerClientsMap, int executionTTLHour) {
    this.executionTTLHour = executionTTLHour;
    this.cluster = cluster;
    this.idToExecutionMap = new LinkedHashMap<>();
    this.fabricToControllerClientsMap = fabricToControllerClientsMap;
    this.executionIdAccessoridAccessor = executionIdAccessoridAccessor;
    idGenerator = new AtomicLong(this.executionIdAccessoridAccessor.getLastGeneratedExecutionId(cluster));
  }

  public AdminCommandExecutionTracker(String cluster, ExecutionIdAccessor executionIdAccessoridAccessor,
      Map<String, ControllerClient> fabricToControllerClientsMap) {
    this(cluster, executionIdAccessoridAccessor, fabricToControllerClientsMap, DEFAULT_TTL_HOUR);
  }

  /**
   * Create an execution context of a command.
   */
  public synchronized AdminCommandExecution createExecution(String operation) {
    return new AdminCommandExecution(generateExecutionId(), operation, cluster, fabricToControllerClientsMap.keySet());
  }

  /**
   * Add execution context into local memory and expired old executions if needed.
   */
  public synchronized void startTrackingExecution(AdminCommandExecution execution) {
    idToExecutionMap.put(execution.getExecutionId(), execution);
    logger.info("Add Execution: " + execution.getExecutionId() + " for operation: " + execution.getOperation()
        + " into tracker.");
    // Try to Collect executions which live longer than TTL.
    int collectedCount = 0;
    Iterator<AdminCommandExecution> iterator = idToExecutionMap.values().iterator();
    LocalDateTime earliestStartTimeToKeep = LocalDateTime.now().minusHours(executionTTLHour);
    while (iterator.hasNext()) {
      AdminCommandExecution oldExecution = iterator.next();
      LocalDateTime commandStartTime = LocalDateTime.parse(oldExecution.getStartTime());
      if (commandStartTime.isBefore(earliestStartTimeToKeep)) {
        // Only collect the execution which is started before the earliest start time to keep and already succeed.
        if (oldExecution.isSucceedInAllFabric()) {
          iterator.remove();
          collectedCount++;
        }
      } else {
        // Execution was started after the earliest start time to keep. Collection complete.
        break;
      }
    }
    logger.info(
        "Collected " + collectedCount + " executions which succeed and were executed before the earliest time to keep:"
            + earliestStartTimeToKeep.toString());
  }

  /**
   * Check the latest status of execution in remote fabrics.
   */
  public synchronized AdminCommandExecution checkExecutionStatus(long id) {
    AdminCommandExecution execution = idToExecutionMap.get(id);
    if (execution == null) {
      return null;
    }
    logger.info("Sending query to remote fabrics to check status of execution: " + id);
    for (Map.Entry<String, ControllerClient> entry : fabricToControllerClientsMap.entrySet()) {
      execution.checkAndUpdateStatusForRemoteFabric(entry.getKey(), entry.getValue());
    }
    logger.info("Updated statuses in remote fabrics for execution: " + id);
    return execution;
  }

  public synchronized AdminCommandExecution getExecution(long id) {
    return idToExecutionMap.get(id);
  }

  protected long generateExecutionId() {
    Long id = idGenerator.addAndGet(1);
    executionIdAccessoridAccessor.updateLastGeneratedExecutionId(cluster, id);
    return id;
  }

  public long getLastExecutionId() {
    return idGenerator.get();
  }

  protected Map<String, ControllerClient> getFabricToControllerClientsMap() {
    return fabricToControllerClientsMap;
  }

  public String executionsAsString(){
    return idToExecutionMap.toString();
  }
}
