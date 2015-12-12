/*******************************************************************************
 * Pentaho Data Science
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.pentaho.di.trans.steps.cpythonscriptexecutor;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision: $
 */
public class CPythonScriptExecutorMetaTest {

  @BeforeClass public static void beforeClass() throws KettlePluginException {
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
  }

  @Test public void testRoundTrips() throws KettleException, NoSuchMethodException, SecurityException {
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_TAG, "getDoingReservoirSampling" );
    getterMap.put( CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_SEED_TAG, "getRandomSeed" );
    getterMap.put( CPythonScriptExecutorMeta.SCRIPT_TAG, "getScript" );
    getterMap.put( CPythonScriptExecutorMeta.INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG,
        "getIncludeFrameRowIndexAsOutputField" );

    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_TAG, "setDoingReservoirSampling" );
    setterMap.put( CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_SEED_TAG, "setRandomSeed" );
    setterMap.put( CPythonScriptExecutorMeta.SCRIPT_TAG, "setScript" );
    setterMap.put( CPythonScriptExecutorMeta.INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG,
        "setIncludeFrameRowIndexAsOutputField" );

    Map<String, FieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorAttributeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorAttributeMap
        .put( CPythonScriptExecutorMeta.OUTPUT_FIELDS_TAG, new CPythonRowMetaInterfaceValidator() );

    Map<String, FieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorTypeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();

    LoadSaveTester
        tester =
        new LoadSaveTester( CPythonScriptExecutorMeta.class, Arrays
            .<String>asList( CPythonScriptExecutorMeta.ROWS_TO_PROCESS_TAG,
                CPythonScriptExecutorMeta.ROWS_TO_PROCESS_SIZE_TAG, CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_TAG,
                CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_SIZE_TAG,
                CPythonScriptExecutorMeta.RESERVOIR_SAMPLING_SEED_TAG, CPythonScriptExecutorMeta.SCRIPT_TAG,
                CPythonScriptExecutorMeta.FRAME_NAMES_TAG, CPythonScriptExecutorMeta.OUTPUT_FIELDS_TAG,
                CPythonScriptExecutorMeta.LOAD_SCRIPT_AT_RUNTIME_TAG, CPythonScriptExecutorMeta.SCRIPT_TO_LOAD_TAG,
                CPythonScriptExecutorMeta.INCLUDE_INPUT_AS_OUTPUT_TAG,
                CPythonScriptExecutorMeta.INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG ), getterMap, setterMap,
            fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    tester.testXmlRoundTrip();
    tester.testRepoRoundTrip();
  }
}
