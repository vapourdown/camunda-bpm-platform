/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
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
package org.camunda.bpm.engine.impl.batch.removaltime;

import static org.camunda.bpm.engine.ProcessEngineConfiguration.HISTORY_REMOVAL_TIME_STRATEGY_END;
import static org.camunda.bpm.engine.ProcessEngineConfiguration.HISTORY_REMOVAL_TIME_STRATEGY_START;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.batch.AbstractBatchJobHandler;
import org.camunda.bpm.engine.impl.batch.BatchJobContext;
import org.camunda.bpm.engine.impl.batch.RepeatableBatchJobDeclaration;
import org.camunda.bpm.engine.impl.cfg.TransactionState;
import org.camunda.bpm.engine.impl.db.DbEntity;
import org.camunda.bpm.engine.impl.db.entitymanager.operation.DbOperation;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.jobexecutor.historycleanup.HistoryCleanupHelper;
import org.camunda.bpm.engine.impl.persistence.entity.ByteArrayEntity;
import org.camunda.bpm.engine.impl.persistence.entity.EverLivingJobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricProcessInstanceEntity;
import org.camunda.bpm.engine.impl.persistence.entity.JobEntity;
import org.camunda.bpm.engine.repository.ProcessDefinition;

/**
 * @author Tassilo Weidner
 */
public class ProcessSetRemovalTimeJobHandler extends AbstractBatchJobHandler<SetRemovalTimeBatchConfiguration> {

  public static final RepeatableBatchJobDeclaration JOB_DECLARATION = new RepeatableBatchJobDeclaration(Batch.TYPE_PROCESS_SET_REMOVAL_TIME);

  @Override
  public void executeHandler(SetRemovalTimeBatchConfiguration batchConfiguration,
                             ExecutionEntity execution,
                             CommandContext commandContext,
                             String tenantId) {
    addRemovalTime(batchConfiguration, commandContext, HistoryCleanupHelper.getHistoryCleanupBatchSize(commandContext));
  }

  protected void addRemovalTime(SetRemovalTimeBatchConfiguration batchConfiguration,
      CommandContext commandContext,
      int batchSize) {
    if (batchConfiguration.isSplitByHistoryTable()) {
      int processIdIndex = batchConfiguration.getProcessIdIndex();
      String instanceId = batchConfiguration.getIds().get(processIdIndex);
      UpdateContext updateContext = new UpdateContext(true, batchSize);
      UpdateResult updateResult = addRemovalTimeToInstance(instanceId, batchConfiguration, updateContext, commandContext);
      if (updateResult == null) {
        // instance not found or removal time exists already, next instance
        if (processIdIndex < batchConfiguration.getIds().size() - 1) {
          batchConfiguration.setProcessIdIndex(processIdIndex + 1);
          addRemovalTime(batchConfiguration, commandContext, batchSize);
        }
      } else {
        JobEntity currentJob = commandContext.getCurrentJob();
        ProcessSetRemovalTimeResultHandler transactionResulthandler = new ProcessSetRemovalTimeResultHandler(updateResult, batchConfiguration,
            currentJob.getId(), commandContext.getProcessEngineConfiguration().getCommandExecutorTxRequiresNew(), this);
        commandContext.getTransactionContext().addTransactionListener(TransactionState.COMMITTED, transactionResulthandler);
      }
    } else {
      batchConfiguration.getIds().forEach(id -> addRemovalTimeToInstance(id, batchConfiguration, new UpdateContext(), commandContext));
    }
  }

  protected UpdateResult addRemovalTimeToInstance(String instanceId,
      SetRemovalTimeBatchConfiguration batchConfiguration,
      UpdateContext updateContext,
      CommandContext commandContext) {
    HistoricProcessInstanceEntity instance = findProcessInstanceById(instanceId, commandContext);
    if (instance != null) {
      if (batchConfiguration.isHierarchical() && hasHierarchy(instance)) {
        String rootProcessInstanceId = instance.getRootProcessInstanceId();
        HistoricProcessInstanceEntity rootInstance = findProcessInstanceById(rootProcessInstanceId, commandContext);
        Date removalTime = getOrCalculateRemovalTime(batchConfiguration, rootInstance, commandContext);
        return addRemovalTimeToHierarchy(rootProcessInstanceId, removalTime, updateContext, commandContext);
      } else {
        Date removalTime = getOrCalculateRemovalTime(batchConfiguration, instance, commandContext);
        if (removalTime != instance.getRemovalTime()) {
          return addRemovalTime(instanceId, removalTime, updateContext, commandContext);
        }
      }
    }
    return null;
  }

  protected Date getOrCalculateRemovalTime(SetRemovalTimeBatchConfiguration batchConfiguration, HistoricProcessInstanceEntity instance, CommandContext commandContext) {
    if (batchConfiguration.hasRemovalTime()) {
      return batchConfiguration.getRemovalTime();

    } else if (hasBaseTime(instance, commandContext)) {
      return calculateRemovalTime(instance, commandContext);

    } else {
      return null;

    }
  }

  protected UpdateResult addRemovalTimeToHierarchy(String rootProcessInstanceId, Date removalTime, UpdateContext updateContext, CommandContext commandContext) {
    UpdateResult updateResult = commandContext.getHistoricProcessInstanceManager()
        .addRemovalTimeToProcessInstancesByRootProcessInstanceId(rootProcessInstanceId, removalTime, updateContext);
    if (isDmnEnabled(commandContext)) {
      return commandContext.getHistoricDecisionInstanceManager()
          .addRemovalTimeToDecisionsByRootProcessInstanceId(rootProcessInstanceId, removalTime, updateContext)
          .addOperations(updateResult.getOperations());
    }
    return updateResult;
  }

  protected UpdateResult addRemovalTime(String instanceId, Date removalTime, UpdateContext updateContext, CommandContext commandContext) {
    UpdateResult updateResult = commandContext.getHistoricProcessInstanceManager()
      .addRemovalTimeById(instanceId, removalTime, updateContext);
    if (isDmnEnabled(commandContext)) {
      return commandContext.getHistoricDecisionInstanceManager()
        .addRemovalTimeToDecisionsByProcessInstanceId(instanceId, removalTime, updateContext)
        .addOperations(updateResult.getOperations());
    }
    return updateResult;
  }

  protected boolean hasBaseTime(HistoricProcessInstanceEntity instance, CommandContext commandContext) {
    return isStrategyStart(commandContext) || (isStrategyEnd(commandContext) && isEnded(instance));
  }

  protected boolean isEnded(HistoricProcessInstanceEntity instance) {
    return instance.getEndTime() != null;
  }

  protected boolean isStrategyStart(CommandContext commandContext) {
    return HISTORY_REMOVAL_TIME_STRATEGY_START.equals(getHistoryRemovalTimeStrategy(commandContext));
  }

  protected boolean isStrategyEnd(CommandContext commandContext) {
    return HISTORY_REMOVAL_TIME_STRATEGY_END.equals(getHistoryRemovalTimeStrategy(commandContext));
  }

  protected boolean hasHierarchy(HistoricProcessInstanceEntity instance) {
    return instance.getRootProcessInstanceId() != null;
  }

  protected String getHistoryRemovalTimeStrategy(CommandContext commandContext) {
    return commandContext.getProcessEngineConfiguration()
      .getHistoryRemovalTimeStrategy();
  }

  protected ProcessDefinition findProcessDefinitionById(String processDefinitionId, CommandContext commandContext) {
    return commandContext.getProcessEngineConfiguration()
      .getDeploymentCache()
      .findDeployedProcessDefinitionById(processDefinitionId);
  }

  protected boolean isDmnEnabled(CommandContext commandContext) {
    return commandContext.getProcessEngineConfiguration().isDmnEnabled();
  }

  protected Date calculateRemovalTime(HistoricProcessInstanceEntity processInstance, CommandContext commandContext) {
    ProcessDefinition processDefinition = findProcessDefinitionById(processInstance.getProcessDefinitionId(), commandContext);

    return commandContext.getProcessEngineConfiguration()
      .getHistoryRemovalTimeProvider()
      .calculateRemovalTime(processInstance, processDefinition);
  }

  protected ByteArrayEntity findByteArrayById(String byteArrayId, CommandContext commandContext) {
    return commandContext.getDbEntityManager()
      .selectById(ByteArrayEntity.class, byteArrayId);
  }

  protected HistoricProcessInstanceEntity findProcessInstanceById(String instanceId, CommandContext commandContext) {
    return commandContext.getHistoricProcessInstanceManager()
      .findHistoricProcessInstance(instanceId);
  }

  @Override
  public JobDeclaration<BatchJobContext, EverLivingJobEntity> getJobDeclaration() {
    return JOB_DECLARATION;
  }

  @Override
  protected SetRemovalTimeBatchConfiguration createJobConfiguration(SetRemovalTimeBatchConfiguration configuration, List<String> processInstanceIds) {
    return new SetRemovalTimeBatchConfiguration(processInstanceIds)
      .setRemovalTime(configuration.getRemovalTime())
      .setHasRemovalTime(configuration.hasRemovalTime())
      .setHierarchical(configuration.isHierarchical())
      .setSplitByHistoryTable(configuration.isSplitByHistoryTable());
  }

  @Override
  protected SetRemovalTimeJsonConverter getJsonConverterInstance() {
    return SetRemovalTimeJsonConverter.INSTANCE;
  }

  @Override
  public String getType() {
    return Batch.TYPE_PROCESS_SET_REMOVAL_TIME;
  }

  public static class UpdateContext {
    protected boolean splitByTable;
    protected int batchSize;

    public UpdateContext() {}
    public UpdateContext(boolean splitByTable, int batchSize) {
      this.splitByTable = splitByTable;
      this.batchSize = batchSize;
    }

    public boolean isSplitByTable() {
      return splitByTable;
    }
    public void setSplitByTable(boolean splitByTable) {
      this.splitByTable = splitByTable;
    }
    public Integer getBatchSize() {
      return isSplitByTable() ? batchSize : null;
    }
    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }

  public static class UpdateResult {
    protected Map<Class<? extends DbEntity>, DbOperation> operations;

    public UpdateResult(Map<Class<? extends DbEntity>, DbOperation> operations) {
      this.operations = operations;
    }

    public Map<Class<? extends DbEntity>, DbOperation> getOperations() {
      return operations;
    }

    public UpdateResult addOperations(Map<Class<? extends DbEntity>, DbOperation> operations) {
      this.operations.putAll(operations);
      return this;
    }

    public boolean isInstanceCompleted(CommandContext commandContext) {
      final int batchSize = HistoryCleanupHelper.getHistoryCleanupBatchSize(commandContext);
      return this.operations.values().stream().noneMatch(op -> op.getRowsAffected() == batchSize);
    }
  }
}
