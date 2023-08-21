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
package org.camunda.bpm.engine.impl.batch;

import org.camunda.bpm.engine.impl.context.Context;
import org.camunda.bpm.engine.impl.core.variable.mapping.value.ConstantValueProvider;
import org.camunda.bpm.engine.impl.core.variable.mapping.value.ParameterValueProvider;
import org.camunda.bpm.engine.impl.jobexecutor.JobDeclaration;
import org.camunda.bpm.engine.impl.jobexecutor.JobHandlerConfiguration;
import org.camunda.bpm.engine.impl.persistence.entity.EverLivingJobEntity;
import org.camunda.bpm.engine.impl.persistence.entity.ExecutionEntity;


public class RepeatableBatchJobDeclaration extends JobDeclaration<BatchJobContext, EverLivingJobEntity> {

  public RepeatableBatchJobDeclaration(String jobHandlerType) {
    super(jobHandlerType);
  }

  @Override
  protected ExecutionEntity resolveExecution(BatchJobContext context) {
    return null;
  }

  @Override
  protected EverLivingJobEntity newJobInstance(BatchJobContext context) {
    return new EverLivingJobEntity();
  }

  @Override
  protected JobHandlerConfiguration resolveJobHandlerConfiguration(BatchJobContext context) {
    return new BatchJobConfiguration(context.getConfiguration().getId());
  }

  @Override
  protected String resolveJobDefinitionId(BatchJobContext context) {
    return context.getBatch().getBatchJobDefinitionId();
  }

  @Override
  public ParameterValueProvider getJobPriorityProvider() {
    long batchJobPriority = Context.getProcessEngineConfiguration()
        .getBatchJobPriority();
    return new ConstantValueProvider(batchJobPriority);
  }

  @Override
  public EverLivingJobEntity reconfigure(BatchJobContext context, EverLivingJobEntity job) {
    super.reconfigure(context, job);
    job.setJobHandlerConfiguration(new BatchJobConfiguration(context.getConfiguration().getId()));
    return job;
  }

}
