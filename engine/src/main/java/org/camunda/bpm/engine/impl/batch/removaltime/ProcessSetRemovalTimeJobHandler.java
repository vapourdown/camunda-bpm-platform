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
import org.camunda.bpm.engine.batch.Batch;
import org.camunda.bpm.engine.impl.batch.AbstractBatchJobHandler;
import org.camunda.bpm.engine.impl.batch.BatchJobContext;
import org.camunda.bpm.engine.impl.batch.RepeatableBatchJobDeclaration;
import org.camunda.bpm.engine.impl.interceptor.CommandContext;
import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.jobexecutor.historycleanup.HistoryCleanupHelper;
import org.camunda.bpm.engine.impl.persistence.entity.ByteArrayEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;
import org.camunda.bpm.engine.impl.persistence.entity.HistoricProcessInstanceEntity;
import org.camunda.bpm.engine.impl.persistence.entity.MessageEntity;
import org.camunda.bpm.engine.impl.persistence.entity.RepeatableBatchJobEntity;
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
    addRemovalTime(batchConfiguration, execution, commandContext, tenantId, HistoryCleanupHelper.getHistoryCleanupBatchSize(commandContext));
  }

  protected void addRemovalTime(SetRemovalTimeBatchConfiguration batchConfiguration,
      ExecutionEntity execution,
      CommandContext commandContext,
      String tenantId,
      int batchSize) {
    if (batchConfiguration.isSplitByHistoryTable()) {
      int processIdIndex = batchConfiguration.getProcessIdIndex();
      String instanceId = batchConfiguration.getIds().get(processIdIndex);
      UpdateContext updateContext = new UpdateContext(true, batchConfiguration.getProcessTableIndex(), batchConfiguration.getDmnTableIndex(), batchSize);
      UpdateResult updateResult = addRemovalTimeToInstance(instanceId, batchConfiguration, updateContext, commandContext);
      if (updateResult == null) {
        // instance not found or removal time exists already, next instance
        if (processIdIndex < batchConfiguration.getIds().size() - 1) {
          batchConfiguration.setProcessIdIndex(processIdIndex + 1);
          batchConfiguration.setProcessTableIndex(0);
          batchConfiguration.setDmnTableIndex(0);
          addRemovalTime(batchConfiguration, execution, commandContext, tenantId, batchSize);
        }
      } else if (updateResult.remainingBatchSize > 0) {
        // current instance done, next instance
        if (processIdIndex < batchConfiguration.getIds().size() - 1) {
          batchConfiguration.setProcessIdIndex(processIdIndex + 1);
          batchConfiguration.setProcessTableIndex(0);
          batchConfiguration.setDmnTableIndex(0);
          addRemovalTime(batchConfiguration, execution, commandContext, tenantId, updateResult.remainingBatchSize);
        }
      } else {
        // current instance not done, limit reached at a table, rerun job with new table index(es)
        batchConfiguration.setProcessTableIndex(updateResult.getProcessTableIndex());
        batchConfiguration.setDmnTableIndex(updateResult.getDmnTableIndex());
        saveConfiguration(commandContext.getByteArrayManager(), batchConfiguration);
        ((RepeatableBatchJobEntity) commandContext.getCurrentJob()).setDoRepeat(true);
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
    if (updateResult.remainingBatchSize > 0 && isDmnEnabled(commandContext)) {
      // process tables are done, update DMN tables for the same instance
      return commandContext.getHistoricDecisionInstanceManager()
          .addRemovalTimeToDecisionsByRootProcessInstanceId(rootProcessInstanceId, removalTime, updateContext);
    }
    return updateResult;
  }

  protected UpdateResult addRemovalTime(String instanceId, Date removalTime, UpdateContext updateContext, CommandContext commandContext) {
    if (updateContext.isSplitByTable()) {

    } else {
      commandContext.getHistoricProcessInstanceManager()
        .addRemovalTimeById(instanceId, removalTime);

      if (isDmnEnabled(commandContext)) {
        commandContext.getHistoricDecisionInstanceManager()
          .addRemovalTimeToDecisionsByProcessInstanceId(instanceId, removalTime);
      }
    }
    return null;
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
  public JobDeclaration<BatchJobContext, MessageEntity> getJobDeclaration() {
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
    protected int processTableIndex;
    protected int dmnTableIndex;
    protected int batchSize;

    public UpdateContext() {}
    public UpdateContext(boolean splitByTable, int processTableIndex, int dmnTableIndex, int batchSize) {
      this.splitByTable = splitByTable;
      this.processTableIndex = processTableIndex;
      this.dmnTableIndex = dmnTableIndex;
      this.batchSize = batchSize;
    }

    public boolean isSplitByTable() {
      return splitByTable;
    }
    public void setSplitByTable(boolean splitByTable) {
      this.splitByTable = splitByTable;
    }
    public int getProcessTableIndex() {
      return processTableIndex;
    }
    public void setProcessTableIndex(int processTableIndex) {
      this.processTableIndex = processTableIndex;
    }
    public int getDmnTableIndex() {
      return dmnTableIndex;
    }
    public void setDmnTableIndex(int dmnTableIndex) {
      this.dmnTableIndex = dmnTableIndex;
    }
    public int getBatchSize() {
      return batchSize;
    }
    public void setBatchSize(int batchSize) {
      this.batchSize = batchSize;
    }
  }

  public static class UpdateResult {
    protected int remainingBatchSize;
    protected int processTableIndex;
    protected int dmnTableIndex;

    public UpdateResult(int remainingBatchSize, int processTableIndex, int dmnTableIndex) {
      this.remainingBatchSize = remainingBatchSize;
      this.processTableIndex = processTableIndex;
      this.dmnTableIndex = dmnTableIndex;
    }

    public int getRemainingBatchSize() {
      return remainingBatchSize;
    }
    public void setRemainingBatchSize(int remainingBatchSize) {
      this.remainingBatchSize = remainingBatchSize;
    }
    public int getProcessTableIndex() {
      return processTableIndex;
    }
    public void setProcessTableIndex(int processTableIndex) {
      this.processTableIndex = processTableIndex;
    }
    public int getDmnTableIndex() {
      return dmnTableIndex;
    }
    public void setDmnTableIndex(int dmnTableIndex) {
      this.dmnTableIndex = dmnTableIndex;
    }
  }
}
