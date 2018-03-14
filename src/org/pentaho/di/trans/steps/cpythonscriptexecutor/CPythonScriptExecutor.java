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
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.steps.reservoirsampling.ReservoirSamplingData;
import org.pentaho.python.PythonSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Step that executes a python script using CPython. The step can accept 0 or more incoming row sets. Row
 * sets are sent to python as named pandas data frames. Data can be sent to python in batches, as samples, row-by-row
 * or as all available rows.
 * </p>
 * Output can be one or more variables that are set in python after the user's script executes. In the case of a single
 * variable this can be a data frame, in which case the columns of the frame become output fields from this step. In the
 * case of multiple variables they are retrieved in string form or as png image data - the step automatically detects if
 * a variable is an image and retrieves it as png. In this mode there is one row output from the step, where each outgoing
 * field holds the string/serializable value of a single variable.
 * </p>
 * The step requires python 2.7 or 3.4. It also requires the pandas, numpy, matplotlib and sklearn. The python executable
 * must be available in the user's path.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class CPythonScriptExecutor extends BaseStep implements StepInterface {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  protected CPythonScriptExecutorData m_data;
  protected CPythonScriptExecutorMeta m_meta;

  protected boolean m_noInputRowSets = false;

  public CPythonScriptExecutor( StepMeta stepMeta, StepDataInterface stepData, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepData, copyNr, transMeta, trans );
  }

  public boolean init( StepMetaInterface stepMeta, StepDataInterface stepData ) {
    if ( super.init( stepMeta, stepData ) ) {
      m_meta = (CPythonScriptExecutorMeta) stepMeta;
      m_data = (CPythonScriptExecutorData) stepData;

      try {
        if ( Const.isEmpty( m_meta.getScript() ) && Const.isEmpty( m_meta.getScriptToLoad() ) ) {
          throw new KettleException( BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.NoScriptProvided" ) );
        }

        if ( m_meta.getFrameNames() != null && m_meta.getFrameNames().size() > 0 ) {
          if ( m_meta.getStepIOMeta().getInfoStreams().size() != m_meta.getFrameNames().size() ) {
            throw new KettleException(
                BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.InputStreamToFrameNameMismatch" ) );
          }
        }

        if ( m_data.m_script == null ) {
          // loading from a file overrides any user-supplied script
          if ( m_meta.getLoadScriptAtRuntime() ) {
            String scriptFile = environmentSubstitute( m_meta.getScriptToLoad() );
            if ( Const.isEmpty( scriptFile ) ) {
              throw new KettleException(
                  BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.NoScriptFileNameProvided" ) );
            }

            m_data.m_script = CPythonScriptExecutorData.loadScriptFromFile( scriptFile );
          } else {
            m_data.m_script = m_meta.getScript();
          }
        }

        if ( !m_data.m_includeInputAsOutput ) {
          m_data.m_includeInputAsOutput = m_meta.getIncludeInputAsOutput();
        }

        // check python availability
        CPythonScriptExecutorData.initPython(this, log);
      } catch ( KettleException ex ) {
        logError( ex.getMessage(), ex ); //$NON-NLS-1$

        return false;
      }

      return true;
    }

    return false;
  }

  @Override public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( first ) {
      first = false;

      List<StreamInterface> infoStreams = m_meta.getStepIOMeta().getInfoStreams();
      RowMetaInterface[] infos = new RowMetaInterface[infoStreams.size()];
      m_data.m_incomingRowSets = new ArrayList<RowSet>();

      if ( infoStreams.size() == 0 ) {
        m_noInputRowSets = true;
      } else {
        String rowsToProcess = m_meta.getRowsToProcess();
        String rowsToProcessSize = environmentSubstitute( m_meta.getRowsToProcessSize() );

        if ( rowsToProcess.equals( BaseMessages
            .getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.BatchEntry.Label" ) ) ) {
          m_data.m_batchSize = Integer.parseInt( rowsToProcessSize.isEmpty() ? "0" : rowsToProcessSize );
        } else if ( rowsToProcess.equals( BaseMessages
            .getString( PKG, "CPythonScriptExecutorDialog.NumberOfRowsToProcess.Dropdown.RowByRowEntry.Label" ) ) ) {
          m_data.m_batchSize = 1;
        } else {
          m_data.m_batchSize = 0;
        }

        String reservoirSamplersSize = environmentSubstitute( m_meta.getReservoirSamplingSize() );
        boolean doingReservoirSampling = m_meta.getDoingReservoirSampling();
        if ( doingReservoirSampling ) {
          m_data.m_reservoirSamplersSize =
              Integer.parseInt( reservoirSamplersSize.isEmpty() ? "0" : reservoirSamplersSize );
        } else {
          m_data.m_reservoirSamplersSize = 0;
        }

        // check for reservoir sampling and set up Reservoirs
        if ( !doingReservoirSampling ) {
          for ( int i = 0; i < infoStreams.size(); i++ ) {
            m_data.m_frameBuffers.add( new ArrayList<Object[]>() );
          }
        } else {
          m_data.m_reservoirSamplers = new ArrayList<ReservoirSamplingData>();
          String seed = environmentSubstitute( m_meta.getRandomSeed() );
          for ( int i = 0; i < infoStreams.size(); i++ ) {
            ReservoirSamplingData rs = new ReservoirSamplingData();
            rs.setProcessingMode( ReservoirSamplingData.PROC_MODE.SAMPLING );
            if ( m_data.m_reservoirSamplersSize < 0 && ( doingReservoirSampling || infoStreams.size() > 1 ) ) {
              // The reservoir sampler is disabled when the sample size is < 0, so we have to
              // set some arbitrarily large sample size in this case in order to simulate the
              // "don't sample, just store all rows" scenario
              m_data.m_reservoirSamplersSize = CPythonScriptExecutorData.DEFAULT_RESERVOIR_SAMPLING_STORE_ALL_ROWS_SIZE;
            }
            rs.initialize( m_data.m_reservoirSamplersSize, seed.isEmpty() ? 0 : Integer.parseInt( seed ) );
            m_data.m_reservoirSamplers.add( rs );

            if ( m_data.m_batchSize == 1 ) { //only the first input frame should be considered
              break;
            }
          }
        }

        for ( int i = 0; i < infoStreams.size(); i++ ) {
          RowSet current = findInputRowSet( infoStreams.get( i ).getStepname() );
          RowMetaInterface associatedRowMeta = getTransMeta().getStepFields( infoStreams.get( i ).getStepname() );

          if ( current == null ) {
            throw new KettleException( BaseMessages
                .getString( PKG, "RScriptExecutor.Error.UnableToFindSpecifiedInputStep",
                    infoStreams.get( i ).getStepname() ) ); //$NON-NLS-1$
          }
          m_data.m_incomingRowSets.add( current );
          infos[i] = associatedRowMeta;

          if ( infos[i] == null ) {
            throw new KettleException( "No row meta for incoming row set " + i ); //$NON-NLS-1$
          }
        }

        m_data.m_finishedRowSets = new boolean[m_data.m_incomingRowSets.size()];
        m_data.m_infoMetas.addAll( Arrays.asList( infos ) );
      }
      m_data.m_outputRowMeta = new RowMeta();
      m_data.m_scriptOnlyOutputRowMeta = new RowMeta();
      m_data.m_incomingFieldsIncludedInOutputRowMeta = new RowMeta();

      m_meta.getFields( m_data.m_outputRowMeta, getStepname(), infos, null, getTransMeta(), null, null );
      m_meta.determineInputFieldScriptFieldSplit( m_data.m_outputRowMeta, m_data.m_scriptOnlyOutputRowMeta,
          m_data.m_incomingFieldsIncludedInOutputRowMeta, infos, getStepname() );
      m_data.initNonScriptOutputIndexLookup();
    }

    if ( isStopped() ) {
      return false;
    }

    boolean allDone = true;
    for ( int i = 0; i < m_data.m_incomingRowSets.size(); i++ ) {
      if ( isStopped() ) {
        return false;
      }

      if ( !m_data.m_finishedRowSets[i] ) {
        RowSet r = m_data.m_incomingRowSets.get( i );
        Object[] row = getRowFrom( r );

        if ( row != null ) {
          allDone = false;

          if ( !m_meta.getDoingReservoirSampling() ) {
            m_data.m_frameBuffers.get( i ).add( row );
          } else {
            m_data.m_reservoirSamplers.get( i ).processRow( row );
          }
        } else {
          m_data.m_finishedRowSets[i] = true;
        }
      }
    }

    processBatch( allDone );

    if ( allDone ) {
      setOutputDone();

      return false;
    }

    if ( checkFeedback( getLinesRead() ) ) {
      logBasic(
          BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.LineNumber", getLinesRead() ) ); //$NON-NLS-1$
    }

    return true;
  }

  protected void processBatch( boolean allDone ) throws KettleException {
    PythonSession session = null;

    try {
      if ( !m_noInputRowSets && !m_meta.getDoingReservoirSampling() && m_data.m_incomingRowSets.size() >= 1 ) {
        boolean framesAdded = false;
        for ( int i = 0; i < m_data.m_frameBuffers.size(); i++ ) {
          List<Object[]> frameBuffer = m_data.m_frameBuffers.get( i );
          if ( frameBuffer.size() == m_data.m_batchSize || ( allDone && frameBuffer.size() > 0 ) ) {
            // push buffer into python and process result
            String frameName = environmentSubstitute( m_meta.getFrameNames().get( i ) );

            logDetailed( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Message.PushingBatchIntoPandasDataFrame", //$NON-NLS-1$
                    frameBuffer.size(), frameName ) );

            session = CPythonScriptExecutorData.acquirePySession( this, getLogChannel(), this );
            rowsToPyDataFrame( session, m_data.m_incomingRowSets.get( i ).getRowMeta(), frameBuffer, frameName );
            framesAdded = true;
          } else {
            framesAdded = false;
          }
        }

        if ( framesAdded ) {
          executeScriptAndProcessResult( session, m_meta.getContinueOnUnsetVars() );
          //clean the current frame buffers
          for ( List<Object[]> frame : m_data.m_frameBuffers ) {
            frame.clear();
          }
        }
      } else if ( !m_noInputRowSets && allDone ) {
        boolean framesAdded = false;
        session = CPythonScriptExecutorData.acquirePySession( this, getLogChannel(), this );

        // grab all the reservoirs an push to python; then process result
        logDetailed( BaseMessages.getString( PKG, "CPythonScriptExecutor.Message.RetrievingReservoirs" ) );
        for ( int j = 0; j < m_data.m_reservoirSamplers.size(); j++ ) {
          ReservoirSamplingData reservoirSamplers = m_data.m_reservoirSamplers.get( j );
          String frameName = environmentSubstitute( m_meta.getFrameNames().get( j ) );
          List<Object[]> sample = reservoirSamplers.getSample();
          CPythonScriptExecutorData.pruneNullRowsFromSample( sample );

          if ( sample != null && sample.size() > 0 ) {
            logDetailed( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Message.PushingSampleFromReservoirIntoPandasDataFrame", j,
                    frameName ) ); //$NON-NLS-1$
            logDetailed( BaseMessages
                .getString( PKG, "CPythonScriptExecutor.Message.SampleSize", sample.size() ) ); //$NON-NLS-1$

            if ( m_data.m_batchSize == 1 ) { //we need to process row by row the sample. we will only have one sample
              List<Object[]> sampleSpliced = new ArrayList<Object[]>();
              for ( int k = 0; k < sample.size(); k++ ) {
                Object[] objects = sample.get( k );
                session = CPythonScriptExecutorData.acquirePySession( this, getLogChannel(), this );
                sampleSpliced.clear();
                sampleSpliced.add( objects );
                rowsToPyDataFrame( session, m_data.m_incomingRowSets.get( j ).getRowMeta(), sampleSpliced, frameName );
                m_data.m_rowByRowReservoirSampleIndex = k;

                executeScriptAndProcessResult( session, m_meta.getContinueOnUnsetVars() );

                if ( session != null ) {
                  CPythonScriptExecutorData.releasePySession( this );
                }
              }

            } else { //process the full sample
              rowsToPyDataFrame( session, m_data.m_incomingRowSets.get( j ).getRowMeta(), sample, frameName );
            }
          }
        }

        if ( m_data.m_batchSize != 1 ) {
          executeScriptAndProcessResult( session, m_meta.getContinueOnUnsetVars() );
        }
      } else if ( m_noInputRowSets ) {
        // just get results from script as we have no inputs to us
        session = CPythonScriptExecutorData.acquirePySession( this, getLogChannel(), this );
        executeScriptAndProcessResult( session, m_meta.getContinueOnUnsetVars() );
      }
    } finally {
      if ( session != null ) {
        PythonSession.releaseSession( this );
      }
    }
  }

  protected void executeScriptAndProcessResult( PythonSession session, boolean continueOnUnsetVars )
      throws KettleException {
    executeScript( session, m_data.m_script );

    Object[][] scriptOutRows = null;
    if ( m_meta.getPythonVariablesToGet().size() == 1 ) {
      // check for existence first...
      if ( !checkIfPythonVariableIsSet( session, m_meta.getPythonVariablesToGet().get( 0 ) ) ) {
        if ( !continueOnUnsetVars ) {
          throw new KettleException( BaseMessages.getString( PKG, "CPythonScriptExecutor.Error.PythonVariableNotSet",
              m_meta.getPythonVariablesToGet().get( 0 ) ) );
        }
      } else {
        // Are we getting a variable (image/text) or a frame?
        PythonSession.PythonVariableType
            type =
            getPythonVariableType( session, m_meta.getPythonVariablesToGet().get( 0 ) );
        Object[][] outputRows = new Object[1][];
        if ( type == PythonSession.PythonVariableType.DataFrame ) {
          outputRows =
              m_data.constructOutputRowsFromFrame( session, m_meta.getPythonVariablesToGet().get( 0 ),
                  m_meta.getIncludeFrameRowIndexAsOutputField(), getLogChannel() );
          includeInputInOutput( outputRows );
        } else {
          outputRows[0] =
              m_data.constructOutputRowNonFrame( session, m_meta.getPythonVariablesToGet(), continueOnUnsetVars,
                  getLogChannel() );
          includeInputInOutput( outputRows );
        }
      }
    } else {
      // more than one variable to get - only non-frame case
      Object[][] outputRows = new Object[1][];
      outputRows[0] =
          m_data.constructOutputRowNonFrame( session, m_meta.getPythonVariablesToGet(), continueOnUnsetVars,
              getLogChannel() );
      includeInputInOutput( outputRows );
    }
  }

  protected void includeInputInOutput( Object[][] outputRows ) throws KettleException {
    if ( !m_meta.getIncludeInputAsOutput() ) {

      for ( Object[] r : outputRows ) {
        putRow( m_data.m_outputRowMeta, r );
      }

      return;
    }

    List<Object[]> flattenedInputRows = new ArrayList<Object[]>();
    int[]
        rowCounts =
        new int[m_meta.getDoingReservoirSampling() ? m_data.m_reservoirSamplers.size() : m_data.m_frameBuffers.size()];
    // RowMetaInterface[] infoMetas = new RowMetaInterface[rowCounts.length];
    int index = 0;
    int sum = 0;
    if ( !m_meta.getDoingReservoirSampling() ) {
      for ( List<Object[]> frameBuffer : m_data.m_frameBuffers ) {
        sum += frameBuffer.size();
        rowCounts[index++] = sum;
        flattenedInputRows.addAll( frameBuffer );
      }
    } else {
      for ( ReservoirSamplingData reservoirSamplingData : m_data.m_reservoirSamplers ) {
        sum += reservoirSamplingData.getSample().size();
        rowCounts[index++] = sum;
        flattenedInputRows.addAll( reservoirSamplingData.getSample() );
      }
    }

    index = 0;
    for ( int i = 0; i < outputRows.length; i++ ) {
      if ( i > rowCounts[index] ) {
        index++;
      }
      // get the input row meta corresponding to this row
      RowMetaInterface associatedRowMeta = m_data.m_infoMetas.get( index );
      if ( outputRows[i] != null ) {
        Object[] inputRow;
        if ( m_data.m_batchSize == 1 && m_meta.getDoingReservoirSampling() ) {
          inputRow = flattenedInputRows.get( m_data.m_rowByRowReservoirSampleIndex );
        } else {
          inputRow = flattenedInputRows.get( i );
        }
        for ( ValueMetaInterface vm : m_data.m_incomingFieldsIncludedInOutputRowMeta.getValueMetaList() ) {
          int outputIndex = m_data.m_nonScriptOutputMetaIndexLookup.get( vm.getName() );
          // is this user selected input field present in the current info input row set?
          int inputIndex = associatedRowMeta.indexOfValue( vm.getName() );
          if ( inputIndex >= 0 ) {
            outputRows[i][outputIndex] = inputRow[inputIndex];
          }
        }
        putRow( m_data.m_outputRowMeta, outputRows[i] );
      }
    }
  }

  protected void executeScript( PythonSession session, String pyScript ) throws KettleException {
    List<String> outAndErr = session.executeScript( environmentSubstitute( pyScript ) );

    // TODO could add another setting to allow the user to specify if the step
    // should try to continue after a script execution error. Note that ServerUtils
    // already logs warning messages and strips them from the error output.
    if ( !Const.isEmpty( outAndErr.get( 1 ) ) ) {
      throw new KettleException( outAndErr.get( 1 ) );
    }
  }

  protected void rowsToPyDataFrame( PythonSession session, RowMetaInterface rowMeta, List<Object[]> rows,
      String pyFrameName ) throws KettleException {
    session.rowsToPythonDataFrame( rowMeta, rows, pyFrameName );
  }

  protected PythonSession.PythonVariableType getPythonVariableType( PythonSession session, String varName )
      throws KettleException {
    return session.getPythonVariableType( varName );
  }

  protected boolean checkIfPythonVariableIsSet( PythonSession session, String varName ) throws KettleException {
    return session.checkIfPythonVariableIsSet( varName );
  }
}
