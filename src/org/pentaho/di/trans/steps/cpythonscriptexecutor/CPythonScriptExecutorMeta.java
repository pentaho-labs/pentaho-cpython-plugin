/*******************************************************************************
 * Pentaho Data Science
 * <p/>
 * Copyright (c) 2002-2017 Hitachi Vantara. All rights reserved.
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

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleAttributeInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepIOMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInjectionMetaEntry;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.ui.trans.steps.cpythonscriptexecutor.CPythonScriptExecutorDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.python.PythonSession;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * Meta class for the CPythonScriptExecutor step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
@Step( id = "CPythonScriptExecutor", image = "pylogo.png", name = "CPython Script Executor", description = "Executes a python script", categoryDescription = "Statistics" )
public class CPythonScriptExecutorMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = CPythonScriptExecutor.class;

  protected static final String ROWS_TO_PROCESS_TAG = "rows_to_process";
  protected static final String ROWS_TO_PROCESS_SIZE_TAG = "rows_to_process_size";
  protected static final String RESERVOIR_SAMPLING_TAG = "reservoir_sampling";
  protected static final String RESERVOIR_SAMPLING_SIZE_TAG = "reservoir_sampling_size";
  protected static final String RESERVOIR_SAMPLING_SEED_TAG = "reservoir_sampling_seed";
  protected static final String INCLUDE_INPUT_AS_OUTPUT_TAG = "include_input_as_output";
  protected static final String INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG = "include_frame_row_index";
  protected static final String LOAD_SCRIPT_AT_RUNTIME_TAG = "load_script_at_runtime";
  protected static final String SCRIPT_TO_LOAD_TAG = "script_to_load";
  protected static final String SCRIPT_TAG = "py_script";
  protected static final String FRAME_NAMES_TAG = "frame_names";
  protected static final String PY_VARS_TO_GET_TAG = "py_vars_to_get";
  protected static final String CONTINUE_ON_UNSET_VARS_TAG = "continue_on_unset_vars";
  protected static final String SINGLE_FRAME_NAME_PREFIX_TAG = "frame_name";
  protected static final String INCOMING_STEP_NAMES_TAG = "incoming_step_names";
  protected static final String SINGLE_INCOMING_STEP_NAME_TAG = "step_name";
  protected static final String OUTPUT_FIELDS_TAG = "output_fields";
  protected static final String SINGLE_OUTPUT_FIELD_TAG = "output_field";

  /**
   * Default prefix for kettle data -> pandas frame name
   */
  public static final String DEFAULT_FRAME_NAME_PREFIX = "kettle_data";

  /**
   * Default row handling strategy
   */
  public static final String
      DEFAULT_ROWS_TO_PROCESS =
      BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label" );

  /**
   * The script to execute
   */
  protected String m_script = BaseMessages.getString( PKG, "CPythonScriptExecutorMeta.InitialScriptText" );

  /**
   * Whether to load a script at runtime
   */
  protected boolean m_loadScriptAtRuntime;

  /**
   * The script to load (if loading at runtime)
   */
  protected String m_loadScriptFile = ""; //$NON-NLS-1$

  /**
   * The name(s) of the data frames to create in python - one corresponding to each incoming row set
   */
  protected List<String> m_frameNames = new ArrayList<String>();

  /**
   * List of variables to get from python. This should hold exactly one variable in the case of extracting a data frame.
   * There can be more than one if all variables are either strings or images
   */
  protected List<String> m_pyVarsToGet = new ArrayList<String>();

  /**
   * Whether to include the pandas frame row index as an output field (when retrieving a single data frame from python
   * as output.
   */
  protected boolean m_includeRowIndex;

  /**
   * Whether to continue processing if one or more requested variables are not set in the python environment after
   * executing the script.
   */
  protected boolean m_continueOnUnsetVars;

  /**
   * Default Rows to Process
   */
  protected String m_rowsToProcess = DEFAULT_ROWS_TO_PROCESS;

  /**
   * Number of rows to process if <code>rows to process is batch </code>
   */
  protected String m_rowsToProcessSize = "";

  /**
   * True if reservoir sampling is to be used, in which case the batch size is the reservoir size
   */
  protected boolean m_doingReservoirSampling = false;

  /**
   * If reservoir sampling is enabled, this value is used to define the sampling size
   */
  protected String m_reservoirSamplingSize = "";

  /**
   * Random seed for reservoir sampling
   */
  protected String m_seed = "1"; //$NON-NLS-1$

  /**
   * True if input stream values should be copied to the output stream. Only applies when
   * output is a single pandas data frame; furthermore, number of output rows must match
   * number of input rows.
   */
  protected boolean m_includeInputAsOutput = false;

  /**
   * Outgoing fields
   */
  protected RowMetaInterface m_outputFields;

  /**
   * Get the output structure
   *
   * @return the output structure
   */
  public RowMetaInterface getOutputFields() {
    return m_outputFields;
  }

  /**
   * Set the output structure
   *
   * @param rm the output structure
   */
  public void setOutputFields( RowMetaInterface rm ) {
    m_outputFields = rm;
  }

  public void setRowsToProcess( String s ) {
    m_rowsToProcess = s;
  }

  public String getRowsToProcess() {
    return m_rowsToProcess;
  }

  public void setRowsToProcessSize( String s ) {
    m_rowsToProcessSize = s;
  }

  public String getRowsToProcessSize() {
    return m_rowsToProcessSize;
  }

  /**
   * Set whether reservoir sampling is to be used in the single input case. Sampling is always used when there are
   * multiple input row sets.
   *
   * @param r true if reservoir sampling is to be used
   */
  public void setDoingReservoirSampling( boolean r ) {
    m_doingReservoirSampling = r;
  }

  /**
   * Get whether reservoir sampling is to be used in the single input case. Sampling is always used when there are
   * multiple input row sets.
   *
   * @return true if reservoir sampling is to be used in the single input case.
   */
  public boolean getDoingReservoirSampling() {
    return m_doingReservoirSampling;
  }

  /**
   * Set the size of the reservoir
   *
   * @param s the size of the reservoir
   */
  public void setReservoirSamplingSize( String s ) {
    m_reservoirSamplingSize = s;
  }

  /**
   * Get the size of the reservoir
   *
   * @return the size of the reservoir
   */
  public String getReservoirSamplingSize() {
    return m_reservoirSamplingSize;
  }

  /**
   * Set the random seed to use for reservoir sampling
   *
   * @param seed the random seed to use when reservoir sampling
   */
  public void setRandomSeed( String seed ) {
    m_seed = seed;
  }

  /**
   * Get the random seed to use for reservoir sampling
   *
   * @return the random seed to use when reservoir sampling
   */
  public String getRandomSeed() {
    return m_seed;
  }

  /**
   * Sets whether the step should or not include input values in the output stream
   */
  public void setIncludeInputAsOutput( boolean s ) {
    m_includeInputAsOutput = s;
  }

  /**
   * Gets whether the step should or not include input values in the output stream
   *
   * @return true if step should include input values in output
   */
  public boolean getIncludeInputAsOutput() {
    return m_includeInputAsOutput;
  }

  /**
   * Set whether to load a script from the file system at runtime rather than executing the user supplied script
   *
   * @param l true if a script is to be loaded at runtime
   */
  public void setLoadScriptAtRuntime( boolean l ) {
    m_loadScriptAtRuntime = l;
  }

  /**
   * Get whether to load a script from the file system at runtime rather than executing the user supplied script
   *
   * @return true if a script is to be loaded at runtime
   */
  public boolean getLoadScriptAtRuntime() {
    return m_loadScriptAtRuntime;
  }

  /**
   * Set the path to the script to load at runtime (if loading at runtime)
   *
   * @param scriptFile the script file to load at runtime
   */
  public void setScriptToLoad( String scriptFile ) {
    m_loadScriptFile = scriptFile;
  }

  /**
   * Get the path to the script to load at runtime (if loading at runtime)
   *
   * @return the script file to load at runtime
   */
  public String getScriptToLoad() {
    return m_loadScriptFile;
  }

  /**
   * Set the python script to execute
   *
   * @param script the script to execute
   */
  public void setScript( String script ) {
    m_script = script;
  }

  /**
   * Get the python script to execute
   *
   * @return the script to execute
   */
  public String getScript() {
    return m_script;
  }

  /**
   * Set the frame names to use when converting incoming row sets into pandas data frames in python. These are the variable names
   * that the user can reference the data by
   *
   * @param names a list of frame names to use - one for each incoming row set
   */
  public void setFrameNames( List<String> names ) {
    m_frameNames = names;
  }

  /**
   * Get the frame names to use when converting incoming row sets into pandas data frames in python. These are the variable names
   * that the user can reference the data by
   *
   * @return a list of frame names to use - one for each incoming row set
   */
  public List<String> getFrameNames() {
    return m_frameNames;
  }

  /**
   * Set the list of python variables to retrieve. If there is more than one variable being retrieved, then each variable
   * will be extracted from python as a string, unless it is an image, in which case the image data is retrieved. The names
   * of fields output by the step are expected to match the variable names in this case; furthermore, the user is expected to set
   * the appropriate outgoing Kettle field type (this must be binary in the case of image data). Note that the step will
   * not know the types of the specified variables before runtime.
   * <p/>
   * If there is just one variable being extracted from python, then the output fields must match the names of the columns
   * of a pandas data frame (in the case that the variable is a data frame), or the name of the variable in the case
   * that is not a data frame. In both cases, appropriate Kettle types must be specified by the user.
   *
   * @param pyVars the list of python variables to retrieve
   */
  public void setPythonVariablesToGet( List<String> pyVars ) {
    m_pyVarsToGet = pyVars;
  }

  /**
   * Get the list of python variables to retrieve. If there is more than one variable being retrieved, then each variable
   * will be extracted from python as a string, unless it is an image, in which case the image data is retrieved. The names
   * of fields output by the step are expected to match the variable names in this case; furthermore, the user is expected to set
   * the appropriate outgoing Kettle field type (this must be binary in the case of image data). Note that the step will
   * not know the types of the specified variables before runtime.
   * <p/>
   * If there is just one variable being extracted from python, then the output fields must match the names of the columns
   * of a pandas data frame (in the case that the variable is a data frame), or the name of the variable in the case
   * that is not a data frame. In both cases, appropriate Kettle types must be specified by the user.
   *
   * @return the list of python variables to retrieve
   */
  public List<String> getPythonVariablesToGet() {
    return m_pyVarsToGet;
  }

  /**
   * Set whether to include the pandas data frame row index as an output field, in the case where the
   * output of the step is a single pandas data frame. Has no affect if multiple variables are being retrieved from
   * python.
   *
   * @param includeFrameRowIndexAsOutputField true to include the frame row index as an output field
   */
  public void setIncludeFrameRowIndexAsOutputField( boolean includeFrameRowIndexAsOutputField ) {
    m_includeRowIndex = includeFrameRowIndexAsOutputField;
  }

  /**
   * Get whether to include the pandas data frame row index as an output field, in the case where the
   * output of the step is a single pandas data frame. Has no affect if multiple variables are being retrieved from
   * python.
   *
   * @return true to include the frame row index as an output field
   */
  public boolean getIncludeFrameRowIndexAsOutputField() {
    return m_includeRowIndex;
  }

  /**
   * Set whether to continue in the case that one or more user specified variables to retrieve are not set in the
   * python environment after executing the script.
   *
   * @param continueOnUnset true to continue processing if there are unset variables
   */
  public void setContinueOnUnsetVars( boolean continueOnUnset ) {
    m_continueOnUnsetVars = continueOnUnset;
  }

  /**
   * Get whether to continue in the case that one or more user specified variables to retrieve are not set in the
   * python environment after executing the script.
   *
   * @return true to continue processing if there are unset variables
   */
  public boolean getContinueOnUnsetVars() {
    return m_continueOnUnsetVars;
  }

  public RowMetaInterface determineOutputRowMeta( RowMetaInterface[] info, VariableSpace space )
      throws KettleException {

    List<RowMetaInterface> incomingMetas = new ArrayList<RowMetaInterface>();
    RowMetaInterface rmi = new RowMeta();

    // possibly multiple incoming row sets
    for ( RowMetaInterface r : info ) {
      if ( r != null ) {
        incomingMetas.add( r );
      }
    }

    PythonSession.RowMetaAndRows
        scriptRM =
        CPythonScriptExecutorData.determineOutputMetaSingleVariable( this, incomingMetas, this, getLog(), space );

    return scriptRM.m_rowMeta;
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String stepName, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repo, IMetaStore metaStore ) throws KettleStepException {

    rowMeta.clear();
    if ( m_outputFields != null && m_outputFields.size() > 0 ) {
      rowMeta.addRowMeta( m_outputFields );

      // Check across all input fields to see if they are in the output, and
      // whether they are binary storage. If binary storage then copy over the original input value meta
      // (this is because get fields in the dialog just creates new ValueMetas without knowledge of storage type)
      if ( getIncludeInputAsOutput() ) {
        for ( RowMetaInterface r : info ) {
          if ( r != null ) {
            for ( ValueMetaInterface vm : r.getValueMetaList() ) {
              int outIndex = m_outputFields.indexOfValue( vm.getName() );
              if ( outIndex >= 0 ) {
                m_outputFields.setValueMeta( outIndex, vm );
              }
            }
          }
        }
      }
    } else {
      int numRowMetas = 0;
      for ( RowMetaInterface r : info ) {
        if ( r != null ) {
          numRowMetas++;
        }
      }
      if ( numRowMetas != m_frameNames.size() ) {
        throw new KettleStepException( BaseMessages
            .getString( PKG, "CPythonScriptExecutorMeta.Error.IncorrectNumberOfIncomingStreams", m_frameNames.size(),
                numRowMetas ) );
      }

      // incoming fields
      addAllIncomingFieldsToOutput( rowMeta, stepName, info );

      // script fields
      try {
        addScriptFieldsToOutput( rowMeta, info, stepName, space );
      } catch ( KettleException ex ) {
        throw new KettleStepException( ex );
      }
    }
  }

  /**
   * Add all incoming fields to the output row meta in the case where no output fields have been defined/edited by
   * the user
   *
   * @param rowMeta
   * @param stepName
   * @param info
   */
  private void addAllIncomingFieldsToOutput( RowMetaInterface rowMeta, String stepName, RowMetaInterface[] info ) {
    if ( getIncludeInputAsOutput() ) {
      for ( RowMetaInterface r : info ) {
        rowMeta.addRowMeta( r );
      }
    }
  }

  /**
   * Add script fields to output row meta in the case where no output fields have been defined/edited by the user. If there
   * is just one variable to extract from python, then the script will be executed on some randomly generated data and
   * the type of the variable will be determined; if it is a pandas frame, then the field meta data can be determined.
   *
   * @param rowMeta
   * @param info
   * @param stepName
   * @param space
   */
  private void addScriptFieldsToOutput( RowMetaInterface rowMeta, RowMetaInterface[] info, String stepName,
      VariableSpace space ) throws KettleException {
    if ( m_pyVarsToGet.size() == 1 ) {
      // could be just a single pandas data frame - see if we can determine
      // the fields in this frame...
      RowMetaInterface scriptRM = determineOutputRowMeta( info, space );

      for ( ValueMetaInterface vm : scriptRM.getValueMetaList() ) {
        vm.setOrigin( stepName );
        rowMeta.addValueMeta( vm );
      }
    } else {
      for ( String varName : m_pyVarsToGet ) {
        // ValueMetaInterface vm = new ValueMeta( varName, ValueMetaInterface.TYPE_STRING );
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta( varName, ValueMetaInterface.TYPE_STRING );
        vm.setOrigin( stepName );
        rowMeta.addValueMeta( vm );
      }
    }
  }

  /**
   * Given a fully defined output row metadata structure, determine which of the output fields are being copied from
   * the input fields and which must be the output of the script.
   *
   * @param fullOutputRowMeta    the fully defined output row metadata structure
   * @param scriptFields         row meta that will hold script only fields
   * @param inputPresentInOutput row meta that will hold input fields being copied
   * @param infos                the array of info row metas
   * @param stepName             the name of the step
   */
  protected void determineInputFieldScriptFieldSplit( RowMetaInterface fullOutputRowMeta, RowMetaInterface scriptFields,
      RowMetaInterface inputPresentInOutput, RowMetaInterface[] infos, String stepName ) {

    scriptFields.clear();
    inputPresentInOutput.clear();
    RowMetaInterface consolidatedInputFields = new RowMeta();
    for ( RowMetaInterface r : infos ) {
      consolidatedInputFields.addRowMeta( r );
    }

    for ( ValueMetaInterface vm : fullOutputRowMeta.getValueMetaList() ) {
      int index = consolidatedInputFields.indexOfValue( vm.getName() );
      if ( index >= 0 ) {
        inputPresentInOutput.addValueMeta( vm );
      } else {
        // must be a script output (either a variable name field or data frame column name
        scriptFields.addValueMeta( vm );
      }
    }
  }

  @Override public void setDefault() {
    m_rowsToProcess =
        BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.AllEntry.Label" );
    m_rowsToProcessSize = "";
    m_doingReservoirSampling = false;
    m_reservoirSamplingSize = "";
    m_frameNames = new ArrayList<String>();
    m_continueOnUnsetVars = false;
    m_pyVarsToGet = new ArrayList<String>();
    m_script = BaseMessages.getString( PKG, "CPythonScriptExecutorMeta.InitialScriptText" ); //$NON-NLS-1$
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta,
      Trans trans ) {
    return new CPythonScriptExecutor( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new CPythonScriptExecutorData();
  }

  @Override protected StepInjectionMetaEntry createEntry( KettleAttributeInterface attr, Class<?> PKG ) {
    return super.createEntry( attr, PKG );
  }

  protected String varListToString() {
    StringBuilder b = new StringBuilder();
    for ( String v : m_pyVarsToGet ) {
      if ( !Const.isEmpty( v.trim() ) ) {
        b.append( v.trim() ).append( "," );
      }
    }

    if ( b.length() > 0 ) {
      b.setLength( b.length() - 1 );
    }

    return b.toString();
  }

  protected void stringToVarList( String list ) {
    m_pyVarsToGet.clear();
    String[] vars = list.split( "," );
    for ( String v : vars ) {
      if ( !Const.isEmpty( v.trim() ) ) {
        m_pyVarsToGet.add( v.trim() );
      }
    }
  }

  @Override public String getXML() {
    StringBuilder buff = new StringBuilder();

    buff.append( XMLHandler.addTagValue( ROWS_TO_PROCESS_TAG, getRowsToProcess() ) );
    buff.append( XMLHandler.addTagValue( ROWS_TO_PROCESS_SIZE_TAG, getRowsToProcessSize() ) );
    buff.append( XMLHandler.addTagValue( RESERVOIR_SAMPLING_TAG, getDoingReservoirSampling() ) );
    buff.append( XMLHandler.addTagValue( RESERVOIR_SAMPLING_SIZE_TAG, getReservoirSamplingSize() ) );
    buff.append( XMLHandler.addTagValue( RESERVOIR_SAMPLING_SEED_TAG, getRandomSeed() ) );
    buff.append( XMLHandler.addTagValue( INCLUDE_INPUT_AS_OUTPUT_TAG, getIncludeInputAsOutput() ) );
    buff.append( XMLHandler.addTagValue( SCRIPT_TAG, getScript() ) );
    buff.append( XMLHandler.addTagValue( LOAD_SCRIPT_AT_RUNTIME_TAG, getLoadScriptAtRuntime() ) );
    buff.append( XMLHandler.addTagValue( SCRIPT_TO_LOAD_TAG, getScriptToLoad() ) );
    buff.append( XMLHandler.addTagValue( CONTINUE_ON_UNSET_VARS_TAG, getContinueOnUnsetVars() ) );
    buff.append( XMLHandler.addTagValue( PY_VARS_TO_GET_TAG, varListToString() ) );
    buff.append(
        XMLHandler.addTagValue( INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG, getIncludeFrameRowIndexAsOutputField() ) );

    // names of the frames to push into python
    buff.append( "   " + XMLHandler.openTag( FRAME_NAMES_TAG ) + Const.CR ); //$NON-NLS-1$
    for ( int i = 0; i < m_frameNames.size(); i++ ) {
      buff.append(
          "    " + XMLHandler.addTagValue( SINGLE_FRAME_NAME_PREFIX_TAG + i, m_frameNames.get( i ) ) ); //$NON-NLS-1$
    }
    buff.append( "    " + XMLHandler.closeTag( FRAME_NAMES_TAG ) + Const.CR ); //$NON-NLS-1$

    // name of the corresponding step that is providing data for each frame
    buff.append( "   " + XMLHandler.openTag( INCOMING_STEP_NAMES_TAG ) + Const.CR ); //$NON-NLS-1$
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    for ( int i = 0; i < infoStreams.size(); i++ ) {
      buff.append( "    " //$NON-NLS-1$
          + XMLHandler.addTagValue( SINGLE_INCOMING_STEP_NAME_TAG + i, infoStreams.get( i ).getStepname() ) );
    }
    buff.append( "   " + XMLHandler.closeTag( INCOMING_STEP_NAMES_TAG ) + Const.CR ); //$NON-NLS-1$

    if ( m_outputFields != null && m_outputFields.size() > 0 ) {
      buff.append( "   " + XMLHandler.openTag( OUTPUT_FIELDS_TAG ) + Const.CR ); //$NON-NLS-1$
      for ( int i = 0; i < m_outputFields.size(); i++ ) {
        ValueMetaInterface vm = m_outputFields.getValueMeta( i );
        buff.append( "        " + XMLHandler.openTag( SINGLE_OUTPUT_FIELD_TAG ) + Const.CR ); //$NON-NLS-1$
        buff.append( "            " + XMLHandler.addTagValue( "field_name", vm.getName() )
            + Const.CR ); //$NON-NLS-1$ //$NON-NLS-2$
        buff.append( "            " + XMLHandler.addTagValue( "type", vm.getTypeDesc() ) ); //$NON-NLS-1$ //$NON-NLS-2$
        buff.append( "        " + XMLHandler.closeTag( SINGLE_OUTPUT_FIELD_TAG ) + Const.CR ); //$NON-NLS-1$
      }
      buff.append( "    " + XMLHandler.closeTag( OUTPUT_FIELDS_TAG ) + Const.CR ); //$NON-NLS-1$
    }

    return buff.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> dbs, IMetaStore metaStore )
      throws KettleXMLException {
    String rowsToProcess = XMLHandler.getTagValue( stepnode, ROWS_TO_PROCESS_TAG );
    setRowsToProcess( rowsToProcess == null ? "" : rowsToProcess );
    String rowsToProcessSize = XMLHandler.getTagValue( stepnode, ROWS_TO_PROCESS_SIZE_TAG );
    setRowsToProcessSize( rowsToProcessSize == null ? "" : rowsToProcessSize );
    setDoingReservoirSampling(
        XMLHandler.getTagValue( stepnode, RESERVOIR_SAMPLING_TAG ).equalsIgnoreCase( "Y" ) ); //$NON-NLS-1$
    String reservoirSamplingSize = XMLHandler.getTagValue( stepnode, RESERVOIR_SAMPLING_SIZE_TAG );
    setReservoirSamplingSize( reservoirSamplingSize == null ? "" : reservoirSamplingSize );
    setRandomSeed( XMLHandler.getTagValue( stepnode, RESERVOIR_SAMPLING_SEED_TAG ) );
    String includeInputAsOutput = XMLHandler.getTagValue( stepnode, INCLUDE_INPUT_AS_OUTPUT_TAG );
    if ( !Const.isEmpty( includeInputAsOutput ) ) {
      setIncludeInputAsOutput( includeInputAsOutput.equalsIgnoreCase( "Y" ) ); //$NON-NLS-1$
    }
    String includeFrameRowIndex = XMLHandler.getTagValue( stepnode, INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG );
    if ( !Const.isEmpty( includeFrameRowIndex ) ) {
      setIncludeFrameRowIndexAsOutputField( includeFrameRowIndex.equalsIgnoreCase( "Y" ) );
    }

    setScript( XMLHandler.getTagValue( stepnode, SCRIPT_TAG ) );

    String loadScript = XMLHandler.getTagValue( stepnode, LOAD_SCRIPT_AT_RUNTIME_TAG );
    if ( !Const.isEmpty( loadScript ) ) {
      setLoadScriptAtRuntime( loadScript.equalsIgnoreCase( "Y" ) ); //$NON-NLS-1$
    }
    setScriptToLoad( XMLHandler.getTagValue( stepnode, SCRIPT_TO_LOAD_TAG ) );

    String continueOnUnset = XMLHandler.getTagValue( stepnode, CONTINUE_ON_UNSET_VARS_TAG );
    if ( !Const.isEmpty( continueOnUnset ) ) {
      setContinueOnUnsetVars( continueOnUnset.equalsIgnoreCase( "Y" ) );
    }

    String pyVars = XMLHandler.getTagValue( stepnode, PY_VARS_TO_GET_TAG );
    if ( !Const.isEmpty( pyVars ) ) {
      stringToVarList( pyVars );
    }

    // get the frame names
    Node frameNameFields = XMLHandler.getSubNode( stepnode, FRAME_NAMES_TAG );
    if ( frameNameFields != null ) {
      Node frameNode = null;
      int i = 0;
      while ( ( frameNode = XMLHandler.getSubNode( frameNameFields, SINGLE_FRAME_NAME_PREFIX_TAG + i ) ) != null ) {
        m_frameNames.add( XMLHandler.getNodeValue( frameNode ) );
        i++;
      }
    }

    // get the step names
    Node stepNameFields = XMLHandler.getSubNode( stepnode, INCOMING_STEP_NAMES_TAG );
    if ( stepNameFields != null ) {
      List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();

      for ( int i = 0; i < infoStreams.size(); i++ ) {
        Node stepNameNode = XMLHandler.getSubNode( stepNameFields, SINGLE_INCOMING_STEP_NAME_TAG + i );
        infoStreams.get( i ).setSubject( XMLHandler.getNodeValue( stepNameNode ) );
      }
    }

    // get the outgoing fields
    Node outgoingFields = XMLHandler.getSubNode( stepnode, OUTPUT_FIELDS_TAG );
    if ( outgoingFields != null && XMLHandler.countNodes( outgoingFields, SINGLE_OUTPUT_FIELD_TAG ) > 0 ) {
      int nrfields = XMLHandler.countNodes( outgoingFields, SINGLE_OUTPUT_FIELD_TAG );

      m_outputFields = new RowMeta();
      for ( int i = 0; i < nrfields; i++ ) {
        Node fieldNode = XMLHandler.getSubNodeByNr( outgoingFields, SINGLE_OUTPUT_FIELD_TAG, i );
        String name = XMLHandler.getTagValue( fieldNode, "field_name" ); //$NON-NLS-1$
        String type = XMLHandler.getTagValue( fieldNode, "type" ); //$NON-NLS-1$
        //ValueMetaInterface vm = new ValueMeta( name, ValueMeta.getType( type ) );
        try {
          ValueMetaInterface vm = ValueMetaFactory.createValueMeta( name, ValueMetaFactory.getIdForValueMeta( type ) );
          m_outputFields.addValueMeta( vm );
        } catch ( KettlePluginException ex ) {
          throw new KettleXMLException( ex );
        }
      }
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> dbs )
      throws KettleException {
    String rowsToProcess = rep.getStepAttributeString( id_step, ROWS_TO_PROCESS_TAG );
    setRowsToProcess( rowsToProcess == null ? "" : rowsToProcess );
    String rowsToProcessSize = rep.getStepAttributeString( id_step, ROWS_TO_PROCESS_SIZE_TAG );
    setRowsToProcessSize( rowsToProcess == null ? "" : rowsToProcessSize );
    setDoingReservoirSampling( rep.getStepAttributeBoolean( id_step, RESERVOIR_SAMPLING_TAG ) );
    String reservoirSamplingSize = rep.getStepAttributeString( id_step, RESERVOIR_SAMPLING_SIZE_TAG );
    setReservoirSamplingSize( reservoirSamplingSize == null ? "" : reservoirSamplingSize );
    setRandomSeed( rep.getStepAttributeString( id_step, RESERVOIR_SAMPLING_SEED_TAG ) );
    setIncludeInputAsOutput( rep.getStepAttributeBoolean( id_step, INCLUDE_INPUT_AS_OUTPUT_TAG ) );
    setIncludeFrameRowIndexAsOutputField(
        rep.getStepAttributeBoolean( id_step, INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG ) );
    setScript( rep.getStepAttributeString( id_step, SCRIPT_TAG ) );
    setLoadScriptAtRuntime( rep.getStepAttributeBoolean( id_step, LOAD_SCRIPT_AT_RUNTIME_TAG ) );
    String scriptToLoad = rep.getStepAttributeString( id_step, SCRIPT_TO_LOAD_TAG );
    setScriptToLoad( Const.isEmpty( scriptToLoad ) ? "" : scriptToLoad ); //$NON-NLS-1$
    setContinueOnUnsetVars( rep.getStepAttributeBoolean( id_step, CONTINUE_ON_UNSET_VARS_TAG ) );
    String pyVars = rep.getStepAttributeString( id_step, PY_VARS_TO_GET_TAG );
    if ( !Const.isEmpty( pyVars ) ) {
      stringToVarList( pyVars );
    }

    // frame names
    int numFields = rep.countNrStepAttributes( id_step, SINGLE_FRAME_NAME_PREFIX_TAG );
    for ( int i = 0; i < numFields; i++ ) {
      m_frameNames.add( rep.getStepAttributeString( id_step, i, SINGLE_FRAME_NAME_PREFIX_TAG ) );
    }

    // step names
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    for ( int i = 0; i < infoStreams.size(); i++ ) {
      infoStreams.get( i ).setSubject( rep.getStepAttributeString( id_step, i, SINGLE_INCOMING_STEP_NAME_TAG ) );
    }

    // get outgoing fields
    numFields = rep.countNrStepAttributes( id_step, "field_name" ); //$NON-NLS-1$
    if ( numFields > 0 ) {
      m_outputFields = new RowMeta();
      for ( int i = 0; i < numFields; i++ ) {
        String name = rep.getStepAttributeString( id_step, i, "field_name" ); //$NON-NLS-1$
        String type = rep.getStepAttributeString( id_step, i, "type" ); //$NON-NLS-1$

        //ValueMetaInterface vm = new ValueMeta( name, ValueMeta.getType( type ) );
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta( name, ValueMetaFactory.getIdForValueMeta( type ) );
        m_outputFields.addValueMeta( vm );
      }
    }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
      throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, ROWS_TO_PROCESS_TAG, getRowsToProcess() );
    rep.saveStepAttribute( id_transformation, id_step, ROWS_TO_PROCESS_SIZE_TAG, getRowsToProcessSize() );
    rep.saveStepAttribute( id_transformation, id_step, RESERVOIR_SAMPLING_TAG, getDoingReservoirSampling() );
    rep.saveStepAttribute( id_transformation, id_step, RESERVOIR_SAMPLING_SIZE_TAG, getReservoirSamplingSize() );
    rep.saveStepAttribute( id_transformation, id_step, RESERVOIR_SAMPLING_SEED_TAG, getRandomSeed() );
    rep.saveStepAttribute( id_transformation, id_step, INCLUDE_INPUT_AS_OUTPUT_TAG, getIncludeInputAsOutput() );
    rep.saveStepAttribute( id_transformation, id_step, SCRIPT_TAG, getScript() );
    rep.saveStepAttribute( id_transformation, id_step, LOAD_SCRIPT_AT_RUNTIME_TAG, getLoadScriptAtRuntime() );
    rep.saveStepAttribute( id_transformation, id_step, SCRIPT_TO_LOAD_TAG, getScriptToLoad() );
    rep.saveStepAttribute( id_transformation, id_step, CONTINUE_ON_UNSET_VARS_TAG, getContinueOnUnsetVars() );
    rep.saveStepAttribute( id_transformation, id_step, PY_VARS_TO_GET_TAG, varListToString() );
    rep.saveStepAttribute( id_transformation, id_step, INCLUDE_FRAME_ROW_INDEX_AS_OUTPUT_FIELD_TAG,
        getIncludeFrameRowIndexAsOutputField() );

    // frame names
    for ( int i = 0; i < m_frameNames.size(); i++ ) {
      rep.saveStepAttribute( id_transformation, id_step, i, SINGLE_FRAME_NAME_PREFIX_TAG, m_frameNames.get( i ) );
    }

    // step names
    List<StreamInterface> infoStreams = getStepIOMeta().getInfoStreams();
    for ( int i = 0; i < infoStreams.size(); i++ ) {
      rep.saveStepAttribute( id_transformation, id_step, i, SINGLE_INCOMING_STEP_NAME_TAG,
          infoStreams.get( i ).getStepname() );
    }

    // outgoing fields
    if ( m_outputFields != null && m_outputFields.size() > 0 ) {
      for ( int i = 0; i < m_outputFields.size(); i++ ) {
        ValueMetaInterface vm = m_outputFields.getValueMeta( i );

        rep.saveStepAttribute( id_transformation, id_step, i, "field_name", vm.getName() ); //$NON-NLS-1$
        rep.saveStepAttribute( id_transformation, id_step, i, "type", vm.getTypeDesc() ); //$NON-NLS-1$
      }
    }
  }

  @Override public Object clone() {
    CPythonScriptExecutorMeta retval = (CPythonScriptExecutorMeta) super.clone();

    return retval;
  }

  public void clearStepIOMeta() {
    ioMeta = null;
  }

  @Override public StepIOMetaInterface getStepIOMeta() {
    if ( ioMeta == null ) {
      ioMeta = new StepIOMeta( true, true, false, false, false, false );
      int numExpectedStreams = m_frameNames.size();

      for ( int i = 0; i < numExpectedStreams; i++ ) {
        ioMeta.addStream(
            new Stream( StreamInterface.StreamType.INFO, null, "Input to pandas frame " + ( i + 1 ), StreamIcon.INFO,
                null ) ); //$NON-NLS-1$
      }
    }

    return ioMeta;
  }

  @Override public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    for ( StreamInterface stream : getStepIOMeta().getInfoStreams() ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  @Override public void resetStepIoMeta() {
    // Don't reset!
  }

  @Override public String getDialogClassName() {
    return CPythonScriptExecutorDialog.class.getCanonicalName();
  }
}
