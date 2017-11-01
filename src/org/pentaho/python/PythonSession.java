/*! ******************************************************************************
 *
 * Pentaho Data Science
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.python;

import org.apache.commons.io.IOUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class implementing a session for interacting with Python
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class PythonSession {

  public static enum PythonVariableType {
    DataFrame, Image, String, Unknown;
  }

  /**
   * Simple container for row meta data and rows
   *
   * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
   */
  public static class RowMetaAndRows {
    public Object[][] m_rows;
    public RowMetaInterface m_rowMeta;
  }

  /**
   * The command used to start python
   */
  private String m_pythonCommand;

  /**
   * The session singleton
   */
  private static PythonSession s_sessionSingleton;

  /**
   * the current session holder
   */
  private static Object s_sessionHolder;

  /**
   * The results of the python check script
   */
  private static String s_pythonEnvCheckResults = "";

  /**
   * For locking
   */
  protected SessionMutex m_mutex = new SessionMutex();

  /**
   * Server socket
   */
  protected ServerSocket m_serverSocket;

  /**
   * Local socket for comms with the python server
   */
  protected Socket m_localSocket;

  /**
   * The process executing the server
   */
  protected Process m_serverProcess;

  /**
   * True when the server has been shutdown
   */
  protected boolean m_shutdown;

  /**
   * A shutdown hook for stopping the server
   */
  protected Thread m_shutdownHook;

  /**
   * PID of the running python server
   */
  protected int m_pythonPID = -1;

  /**
   * The log to use
   */
  protected LogChannelInterface m_log;

  // TODO fix this
  protected String m_kettlePluginDir = "./";

  protected String m_osTmpDir = "";

  /**
   * Acquire the session for the requester
   *
   * @param requester the object requesting the session
   * @return the session singleton
   * @throws SessionException if python is not available
   */
  public static PythonSession acquireSession( Object requester ) throws SessionException {
    return s_sessionSingleton.getSession( requester );
  }

  /**
   * Release the session so that other clients can obtain it. This method does
   * nothing if the requester is not the current session holder
   *
   * @param requester the session holder
   */
  public static void releaseSession( Object requester ) {
    s_sessionSingleton.dropSession( requester );
  }

  /**
   * Returns true if the python environment/server is available
   *
   * @return true if the python environment/server is available
   */
  public static boolean pythonAvailable() {
    return s_sessionSingleton != null;
  }

  protected static File installPyScriptsToTmp() throws IOException {
    ClassLoader loader = PythonSession.class.getClassLoader();
    InputStream in = loader.getResourceAsStream( "py/pyCheck.py" );
    if ( in == null ) {
      throw new IOException( "Unable to read the pyCheck.py script as a resource" );
    }

    String tmpDir = System.getProperty( "java.io.tmpdir" );
    File tempDir = new File( tmpDir );
    String pyCheckDest = tmpDir + File.separator + "pyCheck.py";
    String pyServerDest = tmpDir + File.separator + "pyServer.py";

    PrintWriter outW = null;
    BufferedReader inR = null;
    try {
      outW = new PrintWriter( new BufferedWriter( new FileWriter( pyCheckDest ) ) );
      inR = new BufferedReader( new InputStreamReader( in ) );
      String line;
      while ( ( line = inR.readLine() ) != null ) {
        outW.println( line );
      }
      outW.flush();
      outW.close();
      inR.close();

      in = loader.getResourceAsStream( "py/pyServer.py" );
      outW = new PrintWriter( new BufferedWriter( new FileWriter( pyServerDest ) ) );
      inR = new BufferedReader( new InputStreamReader( in ) );
      while ( ( line = inR.readLine() ) != null ) {
        outW.println( line );
      }
    } finally {
      if ( outW != null ) {
        outW.flush();
        outW.close();
      }
      if ( inR != null ) {
        inR.close();
      }
    }

    return tempDir;
  }

  /**
   * Private constructor
   *
   * @param pythonCommand the command used to start python
   * @throws IOException if a problem occurs
   */
  private PythonSession( String pythonCommand ) throws IOException {
    m_pythonCommand = pythonCommand;
    s_sessionSingleton = null;
    s_pythonEnvCheckResults = "";

    // Read scripts from classpath and write them to tmp.
    /* String
        tester =
        m_kettlePluginDir + File.separator + File.separator + "resources" + File.separator + "py" + File.separator
            + "pyCheck.py"; */
    File tmpDir = installPyScriptsToTmp();
    m_osTmpDir = tmpDir.toString();
    String tester = m_osTmpDir + File.separator + "pyCheck.py";
    ProcessBuilder builder = new ProcessBuilder( pythonCommand, tester );
    Process pyProcess = builder.start();
    StringWriter writer = new StringWriter();
    IOUtils.copy( pyProcess.getInputStream(), writer );
    s_pythonEnvCheckResults = writer.toString();
    s_sessionSingleton = this;
    m_shutdown = false;

    // launch the server socket and python server
    if ( s_pythonEnvCheckResults.length() < 5 ) {
      launchServer( true );
    }
  }

  /**
   * Gets the access to python for a requester. Handles locking.
   *
   * @param requester the requesting object
   * @return the session
   * @throws SessionException if python is not available
   */
  private synchronized PythonSession getSession( Object requester ) throws SessionException {
    if ( s_sessionSingleton == null ) {
      throw new SessionException( "Python not available!" );
    }

    if ( s_sessionHolder == requester ) {
      return this;
    }

    m_mutex.safeLock();
    s_sessionHolder = requester;
    return this;
  }

  /**
   * Release the session for a requester
   *
   * @param requester the requesting object
   */
  private void dropSession( Object requester ) {
    if ( requester == s_sessionHolder ) {
      s_sessionHolder = null;
      m_mutex.unlock();
    }
  }

  /**
   * Launches the python server. Performs some basic requirements checks for the
   * python environment - e.g. python needs to have numpy, pandas and sklearn
   * installed.
   *
   * @param startPython true if the server is to actually be started. False is
   *                    really just for debugging/development where the server can be
   *                    manually started in a separate terminal
   * @throws IOException if a problem occurs
   */
  private void launchServer( boolean startPython ) throws IOException {
    if ( m_log != null ) {
      m_log.logDebug( "Launching server socket..." );
    }
    m_serverSocket = new ServerSocket( 0 );
    m_serverSocket.setSoTimeout( 10000 );
    int localPort = m_serverSocket.getLocalPort();
    if ( m_log != null ) {
      m_log.logDebug( "Local port: " + localPort );
    } else {
      System.err.println( "Local port: " + localPort );
    }
    Thread acceptThread = new Thread() {
      @Override public void run() {
        try {
          m_localSocket = m_serverSocket.accept();
        } catch ( IOException e ) {
          m_localSocket = null;
        }
      }
    };
    acceptThread.start();

    if ( startPython ) {
      /*String
          serverScript =
          m_kettlePluginDir + File.separator + "resources" + File.separator + "py" + File.separator + "pyServer.py"; */
      String serverScript = m_osTmpDir + File.separator + "pyServer.py";
      boolean debug = m_log != null && m_log.isDebug();
      ProcessBuilder
          processBuilder =
          new ProcessBuilder( m_pythonCommand, serverScript, "" + localPort, debug ? "debug" : "" );
      m_serverProcess = processBuilder.start();
    }
    try {
      acceptThread.join();
    } catch ( InterruptedException e ) {
    }

    if ( m_localSocket == null ) {
      shutdown();
      throw new IOException( "Was unable to start python server" );
    } else {
      m_pythonPID = ServerUtils.receiveServerPIDAck( m_localSocket.getInputStream() );

      m_shutdownHook = new Thread() {
        @Override public void run() {
          shutdown();
        }
      };
      Runtime.getRuntime().addShutdownHook( m_shutdownHook );
    }
  }

  public void setLog( LogChannelInterface log ) {
    m_log = log;
  }

  /**
   * Initialize the session. This needs to be called exactly once in order to
   * run checks and launch the server. Creates a session singleton.
   *
   * @param pythonCommand the python command
   * @return true if the server launched successfully
   * @throws KettleException if there was a problem - missing packages in python,
   *                         or python could not be started for some reason
   */
  public static synchronized boolean initSession( String pythonCommand ) throws KettleException {
    if ( s_sessionSingleton != null ) {
      return true;
      // throw new KettleException( BaseMessages.getString( ServerUtils.PKG, "PythonSession.Error.EnvAlreadyAvailable" ) );
    }

    try {
      new PythonSession( pythonCommand );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }

    return s_pythonEnvCheckResults.length() < 5;
  }

  /**
   * Gets the result of running the checks in python
   *
   * @return a string containing the possible errors
   */
  public static String getPythonEnvCheckResults() {
    return s_pythonEnvCheckResults;
  }

  /**
   * Transfer Kettle rows into python as a named pandas data frame
   *
   * @param rowMeta         the metadata of the rows
   * @param rows            the rows to transfer
   * @param pythonFrameName the name of the data frame to use in python
   * @throws KettleException if a problem occurs
   */
  public void rowsToPythonDataFrame( RowMetaInterface rowMeta, List<Object[]> rows, String pythonFrameName )
      throws KettleException {
    try {
      ServerUtils.sendRowsToPandasDataFrame( m_log, rowMeta, rows, pythonFrameName, m_localSocket.getOutputStream(),
          m_localSocket.getInputStream() );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Transfer a pandas data frame from python and convert into Kettle rows and metadata
   *
   * @param frameName       the name of the pandas data frame to get
   * @param includeRowIndex true to include the pandas data frame row index as a field
   * @return rows and row metadata encapsulated in a RowMetaAndRows object
   * @throws KettleException if a problem occurs
   */
  public RowMetaAndRows rowsFromPythonDataFrame( String frameName, boolean includeRowIndex ) throws KettleException {
    try {
      return ServerUtils
          .receiveRowsFromPandasDataFrame( m_log, frameName, includeRowIndex, m_localSocket.getOutputStream(),
              m_localSocket.getInputStream() );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Check if a named variable is set in pythong
   *
   * @param pyVarName the name of the python variable to check for
   * @return true if the named variable exists in the python environment
   * @throws KettleException if a problem occurs
   */
  public boolean checkIfPythonVariableIsSet( String pyVarName ) throws KettleException {
    try {
      return ServerUtils.checkIfPythonVariableIsSet( m_log, pyVarName, m_localSocket.getInputStream(),
          m_localSocket.getOutputStream() );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Grab the contents of the debug buffer from the python server. The server
   * redirects both sys out and sys err to StringIO objects. If debug has been
   * specified, then server debugging output will have been collected in these
   * buffers. Note that the buffers will potentially also contain output from
   * the execution of arbitrary scripts too. Calling this method also resets the
   * buffers.
   *
   * @return the contents of the sys out and sys err streams. Element 0 in the
   * list contains sys out and element 1 contains sys err
   * @throws KettleException if a problem occurs
   */
  public List<String> getPythonDebugBuffer() throws KettleException {
    try {
      return ServerUtils.receiveDebugBuffer( m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Get the type of a variable (according to pre-defined types that we can do something with) in python
   *
   * @param varName the name of the variable to get the type of
   * @return whether the variable is a DataFrame, Image, String or Unknown. DataFrames will be converted to rows; Images
   * (png form) and Strings are output as a single row with fields for each variable value. Unknown types will be
   * returned in their string form.
   * @throws KettleException if a problem occurs
   */
  public PythonVariableType getPythonVariableType( String varName ) throws KettleException {
    try {
      return ServerUtils
          .getPythonVariableType( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Execute a python script.
   *
   * @param pyScript the script to execute
   * @return a List of strings - index 0 contains std out from the script and
   * index 1 contains std err
   * @throws KettleException if a problem occurs
   */
  public List<String> executeScript( String pyScript ) throws KettleException {
    try {
      return ServerUtils
          .executeUserScript( pyScript, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Get an image from python. Assumes that the image is a matplotlib.figure.Figure object. Retrieves this as png
   * data and returns a BufferedImage
   *
   * @param varName the name of the variable containing the image in python
   * @return the image as a BufferedImage
   * @throws KettleException if a problem occurs
   */
  public BufferedImage getImageFromPython( String varName ) throws KettleException {
    try {
      return ServerUtils
          .getPNGImageFromPython( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Get the value of a variable from python in plain text form
   *
   * @param varName the name of the variable to get
   * @return the value of the variable
   * @throws KettleException if a problem occurs
   */
  public String getVariableValueFromPythonAsPlainString( String varName ) throws KettleException {
    try {
      return ServerUtils
          .receivePickledVariableValue( varName, m_localSocket.getOutputStream(), m_localSocket.getInputStream(), true,
              m_log );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Shutdown the python server
   */
  private void shutdown() {
    if ( !m_shutdown ) {
      try {
        m_shutdown = true;
        if ( m_localSocket != null ) {
          if ( m_log == null ) {
            System.err.println( "Sending shutdown command..." );
          } else if ( m_log.isDebug() ) {
            m_log.logDebug( "Sending shutdown command..." );
          }
          if ( m_log == null || m_log.isDebug() ) {
            List<String>
                outAndErr =
                ServerUtils
                    .receiveDebugBuffer( m_localSocket.getOutputStream(), m_localSocket.getInputStream(), m_log );
            if ( outAndErr.get( 0 ).length() > 0 ) {
              if ( m_log == null ) {
                System.err.println( "Python debug std out:\n" + outAndErr.get( 0 ) + "\n" );
              } else {
                m_log.logDebug( "Python debug std out:\n" + outAndErr.get( 0 ) + "\n" );
              }
            }
            if ( outAndErr.get( 1 ).length() > 0 ) {
              if ( m_log == null ) {
                System.err.println( "Python debug std err:\n" + outAndErr.get( 1 ) + "\n" );
              } else {
                m_log.logDebug( "Python debug std err:\n" + outAndErr.get( 1 ) + "\n" );
              }
            }
          }
          ServerUtils.sendServerShutdown( m_localSocket.getOutputStream() );
          m_localSocket.close();
          if ( m_serverProcess != null ) {
            m_serverProcess.destroy();
            m_serverProcess = null;
          }
        }

        if ( m_serverSocket != null ) {
          m_serverSocket.close();
        }
        s_sessionSingleton = null;
      } catch ( Exception ex ) {
        ex.printStackTrace();
        if ( m_pythonPID > 0 ) {
          // try to kill process, just in case
          ProcessBuilder killer;
          if ( System.getProperty( "os.name" ).toLowerCase().contains( "win" ) ) {
            killer = new ProcessBuilder( "taskkill", "/F", "/PID", "" + m_pythonPID );
          } else {
            killer = new ProcessBuilder( "kill", "-9", "" + m_pythonPID );
          }
          try {
            killer.start();
          } catch ( IOException e ) {
            e.printStackTrace();
          }
        }
      } finally {
        if ( m_serverProcess != null ) {
          m_serverProcess.destroy();
        }
      }
    }
  }

  public static void main( String[] args ) {
    try {

/*      File tmp = PythonSession.installPyScriptsToTmp();
      System.err.println( tmp ); */

      if ( !PythonSession.initSession( "python" ) ) {
        System.err.print( "Initialization failed!" );
        System.exit( 1 );
      }

      String temp = "";
      PythonSession session = PythonSession.acquireSession( temp );

      Object[] rowData = { 22, 300.22, "Hello bob", false, new Date(), new Timestamp( new Date().getTime() ), null };
      RowMetaInterface rowMeta = new RowMeta();
      //rowMeta.addValueMeta( new ValueMeta( "Field1", ValueMetaInterface.TYPE_INTEGER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field1", ValueMetaInterface.TYPE_INTEGER ) );
      //rowMeta.addValueMeta( new ValueMeta( "Field2", ValueMetaInterface.TYPE_NUMBER ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field2", ValueMetaInterface.TYPE_NUMBER ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field3", ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field3", ValueMetaInterface.TYPE_STRING ));
      // rowMeta.addValueMeta( new ValueMeta( "Field4", ValueMetaInterface.TYPE_BOOLEAN ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field4", ValueMetaInterface.TYPE_BOOLEAN ));
      // rowMeta.addValueMeta( new ValueMeta( "Field5", ValueMetaInterface.TYPE_DATE ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field5", ValueMetaInterface.TYPE_DATE ) );
      // rowMeta.addValueMeta( new ValueMeta( "Field6", ValueMetaInterface.TYPE_TIMESTAMP ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "Field6", ValueMetaInterface.TYPE_TIMESTAMP ) );
      // rowMeta.addValueMeta( new ValueMeta( "NullField", ValueMetaInterface.TYPE_STRING ) );
      rowMeta.addValueMeta( ValueMetaFactory.createValueMeta( "NullField", ValueMetaInterface.TYPE_STRING ));

      List<Object[]> data = new ArrayList<Object[]>();
      data.add( rowData );
      session.rowsToPythonDataFrame( rowMeta, data, "test" );

      if ( session.checkIfPythonVariableIsSet( "test" ) ) {
        RowMetaAndRows fromPy = session.rowsFromPythonDataFrame( "test", false );
        System.err.println( "Nubmer of field metas returned: " + fromPy.m_rowMeta.size() );
        System.err.println( "Number of rows returned: " + fromPy.m_rows.length );
        for ( ValueMetaInterface v : fromPy.m_rowMeta.getValueMetaList() ) {
          System.err.println( "Col: " + v.getName() + " Type: " + v.getType() );
        }
      } else {
        System.err.println( "Variable 'test' does not seem to be set in python!!!!" );
      }

      session.executeScript( "x = 100\n" );
      if ( session.checkIfPythonVariableIsSet( "x" ) ) {
        System.err.println( "Var x is set!" );
      }
      PythonVariableType t = session.getPythonVariableType( "x" );
      System.err.println( "X is of type " + t.toString() );
    } catch ( Exception ex ) {
      ex.printStackTrace();
    }
  }
}
