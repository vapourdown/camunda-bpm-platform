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

import com.google.gson.JsonObject;
import java.util.Date;
import java.util.List;
import org.camunda.bpm.engine.impl.batch.AbstractBatchConfigurationObjectConverter;
import org.camunda.bpm.engine.impl.batch.DeploymentMappingJsonConverter;
import org.camunda.bpm.engine.impl.batch.DeploymentMappings;
import org.camunda.bpm.engine.impl.util.JsonUtil;

/**
 * @author Tassilo Weidner
 */
public class SetRemovalTimeJsonConverter
    extends AbstractBatchConfigurationObjectConverter<SetRemovalTimeBatchConfiguration> {

  public static final SetRemovalTimeJsonConverter INSTANCE = new SetRemovalTimeJsonConverter();

  protected static final String IDS = "ids";
  protected static final String ID_MAPPINGS = "idMappings";
  protected static final String REMOVAL_TIME = "removalTime";
  protected static final String HAS_REMOVAL_TIME = "hasRemovalTime";
  protected static final String IS_HIERARCHICAL = "isHierarchical";
  protected static final String IS_SPLIT_BY_TABLE = "isSplitByTable";
  protected static final String PROCESS_TABLE_INDEX = "processTableIndex";
  protected static final String DMN_TABLE_INDEX = "dmnTableIndex";
  protected static final String PROCESS_ID_INDEX = "processIdIndex";

  @Override
  public JsonObject writeConfiguration(SetRemovalTimeBatchConfiguration configuration) {
    JsonObject json = JsonUtil.createObject();

    JsonUtil.addListField(json, IDS, configuration.getIds());
    JsonUtil.addListField(json, ID_MAPPINGS, DeploymentMappingJsonConverter.INSTANCE, configuration.getIdMappings());
    JsonUtil.addDateField(json, REMOVAL_TIME, configuration.getRemovalTime());
    JsonUtil.addField(json, HAS_REMOVAL_TIME, configuration.hasRemovalTime());
    JsonUtil.addField(json, IS_HIERARCHICAL, configuration.isHierarchical());
    JsonUtil.addField(json, IS_SPLIT_BY_TABLE, configuration.isSplitByHistoryTable());
    JsonUtil.addField(json, PROCESS_TABLE_INDEX, configuration.getProcessTableIndex());
    JsonUtil.addField(json, DMN_TABLE_INDEX, configuration.getDmnTableIndex());
    JsonUtil.addField(json, PROCESS_ID_INDEX, configuration.getProcessIdIndex());

    return json;
  }

  @Override
  public SetRemovalTimeBatchConfiguration readConfiguration(JsonObject jsonObject) {

    long removalTimeMills = JsonUtil.getLong(jsonObject, REMOVAL_TIME);
    Date removalTime = removalTimeMills > 0 ? new Date(removalTimeMills) : null;

    List<String> instanceIds =  JsonUtil.asStringList(JsonUtil.getArray(jsonObject, IDS));

    DeploymentMappings mappings = JsonUtil.asList(JsonUtil.getArray(jsonObject, ID_MAPPINGS),
        DeploymentMappingJsonConverter.INSTANCE, DeploymentMappings::new);

    boolean hasRemovalTime = JsonUtil.getBoolean(jsonObject, HAS_REMOVAL_TIME);

    boolean isHierarchical = JsonUtil.getBoolean(jsonObject, IS_HIERARCHICAL);

    boolean isSplitByTable = JsonUtil.getBoolean(jsonObject, IS_SPLIT_BY_TABLE);

    int processTableIndex = JsonUtil.getInt(jsonObject, PROCESS_TABLE_INDEX);
    int dmnTableIndex = JsonUtil.getInt(jsonObject, DMN_TABLE_INDEX);
    int processIdIndex = JsonUtil.getInt(jsonObject, PROCESS_ID_INDEX);

    return new SetRemovalTimeBatchConfiguration(instanceIds, mappings)
      .setRemovalTime(removalTime)
      .setHasRemovalTime(hasRemovalTime)
      .setHierarchical(isHierarchical)
      .setSplitByHistoryTable(isSplitByTable)
      .setProcessTableIndex(processTableIndex)
      .setDmnTableIndex(dmnTableIndex)
      .setProcessIdIndex(processIdIndex);
  }

}
