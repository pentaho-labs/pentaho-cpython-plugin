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

import org.apache.commons.vfs2.FileObject;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.reservoirsampling.ReservoirSamplingData;
import org.pentaho.python.PythonSession;
import org.pentaho.python.SessionException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Data class for the CPythonScriptExecutor step
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CPythonScriptExecutorData extends BaseStepData implements StepDataInterface {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  /**
   * number of rows to randomly generate when trying to determine single pandas data frame output
   */
  protected static final int NUM_RANDOM_ROWS = 100;

  /**
   * The reservoir sampling class does not have "store all rows" behavior when the sample size is -1. Instead, it is
   * disabled entirely when sample size < 0. So to simulate this behavior we use a default size for the -1 case. If this
   * is not sufficient, then the user will have to manually set a size that is large enough. Note that Integer.MAX_VALUE
   * is not used because the reservoir class allocates an array list of size equal to the sample size.
   */
  protected static final int DEFAULT_RESERVOIR_SAMPLING_STORE_ALL_ROWS_SIZE = 100000;

  /**
   * Holds the full output row meta data (including any incoming fields that are copied to the outgoing)
   */
  public RowMetaInterface m_outputRowMeta;

  /**
   * Holds output row meta for fields only generated from script execution
   */
  public RowMetaInterface m_scriptOnlyOutputRowMeta;

  /**
   * Holds the row meta for all incoming fields that are getting copied to the output
   */
  public RowMetaInterface m_incomingFieldsIncludedInOutputRowMeta;

  /**
   * The incoming row sets
   */
  protected List<RowSet> m_incomingRowSets;

  /**
   * The list of processed row sets during the getRow of each incoming input stream
   */
  protected boolean[] m_finishedRowSets;

  /**
   * A collection of the frame buffers per input frame
   */
  protected List<List<Object[]>> m_frameBuffers = new ArrayList<List<Object[]>>();

  /**
   * Holds the reservoir samplers
   */
  protected List<ReservoirSamplingData> m_reservoirSamplers = new ArrayList<ReservoirSamplingData>();

  /**
   * Holds the row meta associated with each frame buffer or reservoir sampler
   */
  protected List<RowMetaInterface> m_infoMetas = new ArrayList<RowMetaInterface>();

  /**
   * A index used to reference a line for the incoming rows when we are processing row by row with reservoir sampling
   * active.
   */
  protected int m_rowByRowReservoirSampleIndex;

  /**
   * Batch size
   */
  protected int m_batchSize = 1000;

  /**
   * Reservoir Samplers size
   */
  protected int m_reservoirSamplersSize;

  /**
   * True if input stream values should be copied to the output stream
   */
  protected boolean m_includeInputAsOutput;

  /**
   * The script to run
   */
  protected String m_script;

  /**
   * Lookup for output indexes

   protected Map<String, Integer> m_outputMetaIndexLookup = new HashMap<String, Integer>(); */

  /**
   * Lookup output indexes for just input fields that are being copied to the output
   */
  protected Map<String, Integer> m_nonScriptOutputMetaIndexLookup = new HashMap<String, Integer>();

  /**
   * Variables to retrieve or columns present in pandas data frame that are not defined in the output meta
   */
  protected List<String> m_varsOrColsNotDefinedInOutputMeta = new ArrayList<String>();

  /**
   * Variables or columns defined in the output meta that are not present in the variables to retrieve or columns in the pandas data frame.
   * Script logic (based on input values) could dictate that some variables are not set or dataframe columns not generated for some reason.
   */
  protected List<String> m_varsOrColsInOutputMetaNotPresent = new ArrayList<String>();

  protected boolean m_first = true;

  /**
   * holds any unset variables for a script execution
   */
  protected List<String> m_unsetVars = new ArrayList<String>();

  protected Set<String> m_tmpSet = new HashSet<String>();

  protected static String loadScriptFromFile( String file ) throws KettleException {
    FileObject scriptF = KettleVFS.getFileObject( file );

    BufferedReader br = null;
    StringBuilder b = new StringBuilder();
    try {
      if ( !scriptF.exists() ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "RScriptExecutorData.Error.ScriptFileDoesNotExist", file ) );
      }

      InputStream is = KettleVFS.getInputStream( scriptF );
      InputStreamReader isr = new InputStreamReader( is );
      br = new BufferedReader( isr );

      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        b.append( line ).append( "\n" );
      }

      br.close();
      br = null;
    } catch ( IOException e ) {
      throw new KettleException( e );
    } finally {
      if ( br != null ) {
        try {
          br.close();
          br = null;
        } catch ( IOException e ) {
          throw new KettleException( e );
        }
      }
    }

    return b.toString();
  }

  /**
   * Initialise a lookup on output indexes of any fields being copied from input to output. User may re-order
   * output fields in the dialog for this step, so we need the lookup.
   */
  public void initNonScriptOutputIndexLookup() {
    for ( ValueMetaInterface v : m_incomingFieldsIncludedInOutputRowMeta.getValueMetaList() ) {
      int outIndex = m_outputRowMeta.indexOfValue( v.getName() );
      m_nonScriptOutputMetaIndexLookup.put( v.getName(), outIndex );
    }
  }

  /**
   * Constructs outgoing rows when a single pandas data frame is being retrieved from python
   *
   * @param session         the session to use
   * @param frameName       the name of the frame to get
   * @param includeRowIndex true if the frame's row index is to be an output field (this can be useful in some cases - e.g.
   *                        using pandas routine to compute quantiles of columns stores the quantile value in the index of
   *                        the resulting data frame)
   * @param log             the log to use
   * @return output rows holding the values from the data frame
   * @throws KettleException if a problem occurs
   */
  public Object[][] constructOutputRowsFromFrame( PythonSession session, String frameName, boolean includeRowIndex,
      LogChannelInterface log ) throws KettleException {

    PythonSession.RowMetaAndRows fromPy = session.rowsFromPythonDataFrame( frameName, includeRowIndex );
    RowMetaInterface frameMeta = fromPy.m_rowMeta;
    Object[][] frameRows = fromPy.m_rows;
    Object[][] outputRows = new Object[frameRows.length][];

    if ( log.isDetailed() ) {
      StringBuilder colsNotDefinedInOutputMeta = new StringBuilder();
      StringBuilder colsInOutputMetaNotPresentInFrame = new StringBuilder();

      for ( ValueMetaInterface vm : frameMeta.getValueMetaList() ) {
        if ( m_outputRowMeta.indexOfValue( vm.getName() ) < 0 ) {
          colsNotDefinedInOutputMeta.append( vm.getName() ).append( " " );
        }
      }

      for ( ValueMetaInterface vm : m_outputRowMeta.getValueMetaList() ) {
        if ( !m_nonScriptOutputMetaIndexLookup.containsKey( vm.getName() )
            && frameMeta.indexOfValue( vm.getName() ) < 0 ) {
          colsInOutputMetaNotPresentInFrame.append( vm.getName() ).append( " " );
        }
      }

      if ( colsNotDefinedInOutputMeta.length() > 0 ) {
        log.logDetailed( BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.VarsOrColsNotDefinedInOutputMeta",
            colsNotDefinedInOutputMeta.toString() ) );
      }
      if ( colsInOutputMetaNotPresentInFrame.length() > 0 ) {
        log.logDebug( BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.OutputFieldsNotPresentOrSet",
            colsInOutputMetaNotPresentInFrame.toString() ) );
      }
    }

    for ( int i = 0; i < frameRows.length; i++ ) {
      Object[] frameRow = frameRows[i];
      outputRows[i] = RowDataUtil.allocateRowData( m_outputRowMeta.size() );
      for ( int j = 0; j < frameMeta.size(); j++ ) {
        ValueMetaInterface vmF = frameMeta.getValueMeta( j );
        int outputIndex = m_outputRowMeta.indexOfValue( vmF.getName() );
        if ( outputIndex >= 0 ) {
          outputRows[i][outputIndex] = frameRow[j];
        }
      }
    }

    return outputRows;
  }

  /**
   * Constructs an outgoing row in the case where more than one variable is being extracted from python. In this
   * case there is just one output row after executing the script. Each outgoing field holds the value (either a
   * string or a serializable) of one of the requested python variables.
   *
   * @param session             the session to use
   * @param varsToGet           the list of variables to extract from python
   * @param continueOnUnsetVars true if we should not complain if a requested variable is not set in python after
   *                            the script executes
   * @param log                 the log to use
   * @return an output row
   * @throws KettleException if a problem occurs
   */
  public Object[] constructOutputRowNonFrame( PythonSession session, List<String> varsToGet,
      boolean continueOnUnsetVars, LogChannelInterface log ) throws KettleException {
    Object[] outputRow = RowDataUtil.allocateRowData( m_outputRowMeta.size() );

    // check var names against output meta
    if ( m_first ) {
      // check for vars that are not defined in output meta
      for ( String v : varsToGet ) {
        // if ( !m_outputMetaIndexLookup.containsKey( v ) ) {
        if ( m_outputRowMeta.indexOfValue( v ) < 0 ) {
          m_varsOrColsNotDefinedInOutputMeta.add( v );
        }
      }

      if ( m_varsOrColsNotDefinedInOutputMeta.size() > 0 && log != null ) {
        StringBuilder b = new StringBuilder();
        for ( String v : m_varsOrColsNotDefinedInOutputMeta ) {
          b.append( v ).append( " " );
        }
        log.logDetailed( BaseMessages
            .getString( PKG, "CPythonScriptExecutor.Message.VarsOrColsNotDefinedInOutputMeta", b.toString() ) );
      }
      m_first = false;
    }

    m_tmpSet.clear();
    m_unsetVars.clear();

    // get the values of the variables
    for ( String v : varsToGet ) {
      if ( session.checkIfPythonVariableIsSet( v ) ) {
        // add this to the ok list, so we can check to see if there
        // are vars defined in output meta that are not set or not
        // present in the list of vars to get
        m_tmpSet.add( v );

        int outputIndex = m_outputRowMeta.indexOfValue( v );
        if ( outputIndex >= 0 ) {
          PythonSession.PythonVariableType varType = session.getPythonVariableType( v );
          if ( varType == PythonSession.PythonVariableType.Image ) {
            if ( m_outputRowMeta.getValueMeta( outputIndex ).getType() != ValueMetaInterface.TYPE_SERIALIZABLE ) {
              throw new KettleException(
                  BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.ImageDataMustBeStoredInSerializable" ) );
            }
            outputRow[outputIndex] = session.getImageFromPython( v );
          } else {
            Object varVal = session.getVariableValueFromPythonAsPlainString( v );
            if ( m_outputRowMeta.getValueMeta( outputIndex ).getType() != ValueMetaInterface.TYPE_STRING ) {
              varVal = m_outputRowMeta.getValueMeta( outputIndex )
                  //.convertData( new ValueMeta( v, ValueMetaInterface.TYPE_STRING ), varVal );
                  .convertData( ValueMetaFactory.createValueMeta( v, ValueMetaInterface.TYPE_STRING ), varVal );
            }
            outputRow[outputIndex] = varVal;
          }
        }
      } else {
        if ( !continueOnUnsetVars ) {
          m_unsetVars.add( v );
        }
      }
    }

    if ( m_unsetVars.size() > 0 ) {
      StringBuilder b = new StringBuilder();
      for ( String v : m_unsetVars ) {
        b.append( v ).append( " " );
      }
      throw new KettleException(
          BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonVariableNotSet", b.toString() ) );
    }

    if ( m_tmpSet.size() != m_outputRowMeta.size() ) {
      StringBuilder b = new StringBuilder();
      for ( ValueMetaInterface outV : m_scriptOnlyOutputRowMeta.getValueMetaList() ) {
        if ( !m_tmpSet.contains( outV.getName() ) ) {
          b.append( outV.getName() ).append( " " );
        }
      }
      if ( log != null ) {
        log.logDetailed(
            BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.OutputFieldsNotPresentOrSet", b.toString() ) );
      }
    }

    return outputRow;
  }

  /**
   * Remove any null entries from a sample
   *
   * @param sample the sample to check.
   */
  public static void pruneNullRowsFromSample( List<Object[]> sample ) {
    int pos = sample.size() - 1;

    // remove from the end to avoid internal shifting.
    while ( pos > 0 && sample.get( pos ) == null ) {
      sample.remove( pos );
      pos--;
    }
  }

  /**
   * Generate some random rows to send to python in the case where a single variable (data frame) is being extracted
   * and we want to try and determine the types of the output fields
   *
   * @param inputMeta incoming row meta
   * @param r         Random instance to use
   * @return a list of randomly generated rows with types matching the incoming row types.
   * @throws KettleException if a problem occurs
   */
  protected static List<Object[]> generateRandomRows( RowMetaInterface inputMeta, Random r ) throws KettleException {
    List<Object[]> rows = new ArrayList<Object[]>( NUM_RANDOM_ROWS );
    // ValueMetaInterface numericVM = new ValueMeta( "num", ValueMetaInterface.TYPE_NUMBER ); //$NON-NLS-1$
    ValueMetaInterface
        numericVM =
        ValueMetaFactory.createValueMeta( "num", ValueMetaInterface.TYPE_NUMBER ); //$NON-NLS-1$

    for ( int i = 0; i < NUM_RANDOM_ROWS; i++ ) {
      Object[] currentRow = new Object[inputMeta.size()];
      for ( int j = 0; j < inputMeta.size(); j++ ) {
        ValueMetaInterface vm = inputMeta.getValueMeta( j );

        ValueMetaInterface tempVM = vm.clone();
        tempVM.setStorageType( ValueMetaInterface.STORAGE_TYPE_NORMAL );

        Object newVal;
        double d = r.nextDouble();
        switch ( vm.getType() ) {
          case ValueMetaInterface.TYPE_NUMBER:
          case ValueMetaInterface.TYPE_INTEGER:
          case ValueMetaInterface.TYPE_BIGNUMBER:
            d *= 100.0;
            newVal = d;
            if ( vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_BINARY_STRING ) {
              newVal = tempVM.convertData( numericVM, newVal );
            }
            currentRow[j] =
                vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_NORMAL ? vm.convertData( numericVM, newVal ) :
                    tempVM.convertToBinaryStringStorageType( newVal );
            break;
          case ValueMetaInterface.TYPE_DATE:
            newVal = new Date( new Date().getTime() + (long) ( d * 100000 ) );
            currentRow[j] =
                vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_NORMAL ? newVal :
                    tempVM.convertToBinaryStringStorageType( newVal );
            break;
          case ValueMetaInterface.TYPE_TIMESTAMP:
            newVal = new Timestamp( new Date().getTime() + (long) ( d * 100000 ) );
            currentRow[j] =
                vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_NORMAL ? newVal :
                    tempVM.convertToBinaryStringStorageType( newVal );
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            newVal = r.nextBoolean();
            currentRow[j] =
                vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_NORMAL ? newVal :
                    tempVM.convertToBinaryStringStorageType( newVal );
            break;
          default:
            newVal = d < 0.5 ? "value1" : "value2";
            currentRow[j] =
                vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_NORMAL ? newVal :
                    tempVM.convertToBinaryStringStorageType( newVal );
        }
      }
      rows.add( currentRow );
    }
    return rows;
  }

  public static PythonSession.RowMetaAndRows determineOutputMetaSingleVariable( Object requester,
      List<RowMetaInterface> inputMetas, CPythonScriptExecutorMeta cPythonScriptExecutorMeta, LogChannelInterface log,
      VariableSpace vars ) throws KettleException {

    synchronized ( requester ) {
      PythonSession.RowMetaAndRows outputMeta = null;
      PythonSession session = null;
      try {
        String varToGet = vars.environmentSubstitute( cPythonScriptExecutorMeta.getPythonVariablesToGet().get( 0 ) );

        String script = cPythonScriptExecutorMeta.getScript();
        if ( cPythonScriptExecutorMeta.getLoadScriptAtRuntime() ) {
          String fileName = vars.environmentSubstitute( cPythonScriptExecutorMeta.getScriptToLoad() );
          if ( Const.isEmpty( fileName ) ) {
            throw new KettleException(
                BaseMessages.getString( PKG, "CPythonScriptExecutorData.Error.ScriptFileDoesNotExist", fileName ) );
          }

          script = loadScriptFromFile( fileName );
        }

        List<String> frameNames = cPythonScriptExecutorMeta.getFrameNames();
        if ( Const.isEmpty( script ) ) {
          throw new KettleException(
              BaseMessages.getString( PKG, "CPythonScriptExecutorData.Error.CantDetermineOutputMeta" ) );
        }

        if ( inputMetas != null && inputMetas.size() != frameNames.size() ) {
          throw new KettleException(
              BaseMessages.getString( PKG, "CPythonScriptExecutorData.Error.WrongNumberOfFrameNames" ) );
        }

        log.logDetailed( BaseMessages.getString( PKG, "CPythonScriptExecutorData.Message.DeterminingOutputFormat" ) );

        Random r = new Random( 1 );

        session = acquirePySession( requester, log, vars );

        List<List<Object[]>> randomRows = new ArrayList<List<Object[]>>();
        if ( inputMetas != null ) {
          for ( int i = 0; i < inputMetas.size(); i++ ) {
            RowMetaInterface currentMeta = inputMetas.get( i );
            String currentFrameName = vars.environmentSubstitute( frameNames.get( i ) );
            List<Object[]> randomRow = generateRandomRows( currentMeta, r );
            randomRows.add( randomRow );
            session.rowsToPythonDataFrame( currentMeta, randomRow, currentFrameName );
          }
        }

        List<String> outAndErrors = session.executeScript( script );
        if ( !Const.isEmpty( outAndErrors.get( 1 ) ) ) {
          throw new KettleException( outAndErrors.get( 1 ) );
        }
        if ( !session.checkIfPythonVariableIsSet( varToGet ) ) {
          throw new KettleException(
              BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonVariableNotSet", varToGet ) );
        }

        // determine type...
        PythonSession.PythonVariableType type = session.getPythonVariableType( varToGet );
        if ( type == PythonSession.PythonVariableType.DataFrame ) {
          PythonSession.RowMetaAndRows
              result =
              session.rowsFromPythonDataFrame( varToGet,
                  cPythonScriptExecutorMeta.getIncludeFrameRowIndexAsOutputField() );
          return result;
        } else {
          // this variable is some other type
          ValueMetaInterface vm = type == PythonSession.PythonVariableType.Image ?
              //new ValueMeta( varToGet, ValueMetaInterface.TYPE_SERIALIZABLE ) :
              ValueMetaFactory.createValueMeta( varToGet, ValueMetaInterface.TYPE_SERIALIZABLE ) :
              //new ValueMeta( varToGet, ValueMetaInterface.TYPE_STRING );
              ValueMetaFactory.createValueMeta( varToGet, ValueMetaInterface.TYPE_STRING );
          PythonSession.RowMetaAndRows result = new PythonSession.RowMetaAndRows();
          result.m_rowMeta = new RowMeta();
          result.m_rowMeta.addValueMeta( vm );
          return result;
        }
      } catch ( Exception ex ) {
        throw new KettleException( ex );
      } finally {
        releasePySession( requester );
      }
    }
  }

  public static void initPython( VariableSpace vars, LogChannelInterface log ) throws KettleException {
    // check python availability
    if ( !PythonSession.pythonAvailable() ) {
      // initialize...
      PythonSession.initSession( "python", vars, log );
    } else {
      return;
    }
    if ( !PythonSession.pythonAvailable() ) {
      String pyCheckResults = PythonSession.getPythonEnvCheckResults();
      if ( !Const.isEmpty( pyCheckResults ) ) {
        throw new KettleException(
            BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonInitializationProblem" ) + ":\n\n"
                + pyCheckResults );
      } else {
        throw new KettleException(
            BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonInitializationProblem" ) );
      }
    }
  }

  public static PythonSession acquirePySession( Object requester, LogChannelInterface log, VariableSpace vars )
      throws KettleException {
    // check availability first...
    initPython( vars, log );

    PythonSession session;
    try {
      session = PythonSession.acquireSession( requester );
    } catch ( SessionException ex ) {
      throw new KettleException( ex );
    }

    session.setLog( log );
    return session;
  }

  protected static void releasePySession( Object requester ) {
    PythonSession.releaseSession( requester );
  }
}
