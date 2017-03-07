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

import com.opencsv.CSVParser;
import org.apache.commons.codec.binary.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.python.PythonSession.PythonVariableType;
import org.pentaho.python.PythonSession.RowMetaAndRows;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Routines for communicating with the python server
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class ServerUtils {

  public static Class<?> PKG = ServerUtils.class;

  protected static final String COMMAND_KEY = "command";
  protected static final String DEBUG_KEY = "debug";
  protected static final String NUM_ROWS_KEY = "num_rows";
  protected static final String ROW_META_KEY = "row_meta";
  protected static final String RESPONSE_KEY = "response";
  protected static final String OK_KEY = "ok";
  protected static final String ERROR_MESSAGE_KEY = "error_message";
  protected static final String VARIABLE_NAME_KEY = "variable_name";
  protected static final String VARIABLE_IS_SET_KEY = "variable_is_set";
  protected static final String VARIABLE_EXISTS_KEY = "variable_exists";
  protected static final String VARIABLE_ENCODING_KEY = "variable_encoding";
  protected static final String VARIABLE_VALUE_KEY = "variable_value";
  protected static final String GET_VARIABLE_TYPE_KEY = "get_variable_type";
  protected static final String GET_VARIABLE_VALUE_KEY = "get_variable_value";
  protected static final String GET_IMAGE_KEY = "get_image";
  protected static final String IMAGE_ENCODING_KEY = "encoding";
  protected static final String IMAGE_DATA_KEY = "image_data";
  protected static final String BASE64_ENCODING_KEY = "base64";
  protected static final String STRING_ENCODING_KEY = "string";
  protected static final String JSON_ENCODING_KEY = "json";
  protected static final String PICKLED_ENCODING_KEY = "pickled";
  protected static final String VARIABLE_TYPE_RESPONSE_KEY = "type";
  protected static final String PID_RESPONSE_KEY = "pid_response";
  protected static final String SCRIPT_OUT_KEY = "script_out";
  protected static final String SCRIPT_ERROR_KEY = "script_error";

  protected static final String FRAME_NAME_KEY = "frame_name";
  protected static final String FRAME_INCLUDE_ROW_INDEX = "include_index";
  protected static final String FIELDS_KEY = "fields";
  protected static final String FIELD_NAME_KEY = "name";
  protected static final String FIELD_TYPE_KEY = "type";
  protected static final String FIELD_TYPE_NUMBER = "number";
  protected static final String FIELD_TYPE_DATE = "date";
  protected static final String FIELD_TYPE_STRING = "string";
  protected static final String FIELD_TYPE_BOOLEAN = "boolean";
  protected static final String FIELD_DATE_FORMAT = "date_format";
  protected static final String FIELD_DATE_FORMAT_NONE = "nome";

  protected static final String ACCEPT_ROWS_COMMAND = "accept_rows";
  protected static final String GET_FRAME_COMMAND = "get_frame";
  protected static final String EXECUTE_SCRIPT_COMMAND = "execute_script";

  protected static final String MISSING_VALUE = "?";

  /**
   * For parsing CSV rows from python
   */
  protected static final CSVParser PARSER = new CSVParser( ',', '\'', '\\' );

  /**
   * For parsing dates out of CSV returned from python
   */
  protected static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );

  protected static Map<String, Object> createMetadataMessage( String frameName, RowMetaInterface meta ) {
    Map<String, Object> result = new HashMap<String, Object>();
    result.put( FRAME_NAME_KEY, frameName );
    List<Map<String, String>> fields = new ArrayList<Map<String, String>>();

    result.put( FIELDS_KEY, fields );
    for ( ValueMetaInterface v : meta.getValueMetaList() ) {
      Map<String, String> fieldMeta = new HashMap<String, String>();
      fieldMeta.put( FIELD_NAME_KEY, v.getName() );
      switch ( v.getType() ) {
        case ValueMetaInterface.TYPE_NUMBER:
        case ValueMetaInterface.TYPE_INTEGER:
        case ValueMetaInterface.TYPE_BIGNUMBER:
          fieldMeta.put( FIELD_TYPE_KEY, FIELD_TYPE_NUMBER );
          break;
        case ValueMetaInterface.TYPE_DATE:
        case ValueMetaInterface.TYPE_TIMESTAMP:
          fieldMeta.put( FIELD_TYPE_KEY, FIELD_TYPE_DATE );
          String format = v.getDateFormat().toString();
          if ( Const.isEmpty( format ) ) {
            format = FIELD_DATE_FORMAT_NONE;
          }
          fieldMeta.put( FIELD_DATE_FORMAT, format );
          break;
        case ValueMetaInterface.TYPE_BOOLEAN:
          fieldMeta.put( FIELD_TYPE_KEY, FIELD_TYPE_BOOLEAN );
          break;
        // TODO throw an exception for Serializable/Binary
        default:
          fieldMeta.put( FIELD_TYPE_KEY, FIELD_TYPE_STRING );
          break;
      }
      fields.add( fieldMeta );
    }

    return result;
  }

  /**
   * Check if a named variable is set in python
   *
   * @param log          the log channel to use
   * @param varName      the name of the variable to check
   * @param inputStream  the input stream to read a response from
   * @param outputStream the output stream to talk to the server on
   * @return true if the named variable is set in python
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static boolean checkIfPythonVariableIsSet( LogChannelInterface log,
      String varName, InputStream inputStream, OutputStream outputStream ) throws KettleException {

    boolean debug = log == null || log.isDebug();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, VARIABLE_IS_SET_KEY );
    command.put( VARIABLE_NAME_KEY, varName );
    command.put( DEBUG_KEY, debug );

    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }

        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        bytes = readDelimitedFromInputStream( inputStream );
        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( RESPONSE_KEY ).toString().equals( OK_KEY ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }

        if ( !ack.get( VARIABLE_NAME_KEY ).toString().equals( varName ) ) {
          throw new KettleException( "Server sent back a response for a different " + "variable!" );
        }

        return (Boolean) ack.get( VARIABLE_EXISTS_KEY );
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else {
      outputCommandDebug( command, log );
    }

    return false;
  }

  /**
   * Receive the value of a variable in pickled or plain string form. If getting
   * a pickled variable, then in python 2 this is the pickled string; in python
   * 3 pickle.dumps returns a byte object, so the value is converted to base64
   * before leaving the server.
   *
   * @param varName      the name of the variable to get from the server
   * @param outputStream the output stream to write to
   * @param inputStream  the input stream to get server responses from
   * @param plainString  true if the plain string form of the variable is to be
   *                     returned; otherwise the variable value is pickled (and further
   *                     encoded to base64 in the case of python 3)
   * @param log          optional log
   * @return the variable value
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static String receivePickledVariableValue( String varName,
      OutputStream outputStream, InputStream inputStream, boolean plainString, LogChannelInterface log )
      throws KettleException {

    boolean debug = log == null || log.isDebug();
    String objectValue = "";
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, GET_VARIABLE_VALUE_KEY );
    command.put( VARIABLE_NAME_KEY, varName );
    command.put( VARIABLE_ENCODING_KEY, plainString ? STRING_ENCODING_KEY : PICKLED_ENCODING_KEY );
    command.put( DEBUG_KEY, debug );

    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }

        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        bytes = readDelimitedFromInputStream( inputStream );
        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( RESPONSE_KEY ).toString().equals( OK_KEY ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }
        if ( !ack.get( VARIABLE_NAME_KEY ).toString().equals( varName ) ) {
          throw new KettleException( "Server sent back a value for a different " + "variable!" );
        }
        objectValue = ack.get( VARIABLE_VALUE_KEY ).toString();
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else if ( debug ) {
      outputCommandDebug( command, log );
    }

    return objectValue;
  }

  /**
   * Get an image from python. Assumes that the image is a
   * matplotlib.figure.Figure object. Retrieves this as png data and returns a
   * BufferedImage.
   *
   * @param varName      the name of the variable containing the image in python
   * @param outputStream the output stream to talk to the server on
   * @param inputStream  the input stream to receive server responses from
   * @param log          an optional log
   * @return a BufferedImage
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static BufferedImage getPNGImageFromPython( String varName,
      OutputStream outputStream, InputStream inputStream, LogChannelInterface log ) throws KettleException {

    boolean debug = log == null || log.isDebug();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, GET_IMAGE_KEY );
    command.put( VARIABLE_NAME_KEY, varName );
    command.put( DEBUG_KEY, debug );

    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }
        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        bytes = readDelimitedFromInputStream( inputStream );

        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( RESPONSE_KEY ).toString().equals( OK_KEY ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }

        if ( !ack.get( VARIABLE_NAME_KEY ).toString().equals( varName ) ) {
          throw new KettleException( "Server sent back a response for a different " + "variable!" );
        }

        String encoding = ack.get( IMAGE_ENCODING_KEY ).toString();
        String imageData = ack.get( IMAGE_DATA_KEY ).toString();
        byte[] imageBytes;
        if ( encoding.equals( BASE64_ENCODING_KEY ) ) {
          imageBytes = Base64.decodeBase64( imageData.getBytes() );
        } else {
          imageBytes = imageData.getBytes();
        }
        return ImageIO.read( new BufferedInputStream( new ByteArrayInputStream( imageBytes ) ) );
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else {
      outputCommandDebug( command, log );
    }
    return null;
  }

  /**
   * Execute a script on the server
   *
   * @param script       the script to execute
   * @param outputStream the output stream to write data to the server
   * @param inputStream  the input stream to read responses from
   * @param log          optional log to write to
   * @return a two element list that contains the sys out and sys error from the
   * script execution
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static List<String> executeUserScript( String script,
      OutputStream outputStream, InputStream inputStream, LogChannelInterface log ) throws KettleException {
    if ( !script.endsWith( "\n" ) ) {
      script += "\n";
    }
    List<String> outAndErr = new ArrayList<String>();

    ObjectMapper mapper = new ObjectMapper();
    boolean debug = log == null || log.isDebug();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( "command", "execute_script" );
    command.put( "script", script );
    command.put( "debug", debug );
    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }
        writeDelimitedToOutputStream( bytes, outputStream );

        // get the result of execution
        bytes = readDelimitedFromInputStream( inputStream );

        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( RESPONSE_KEY ).toString().equals( OK_KEY ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }
        // get the script out and err
        outAndErr.add( ack.get( SCRIPT_OUT_KEY ).toString() );
        outAndErr.add( ack.get( SCRIPT_ERROR_KEY ).toString() );
        if ( debug ) {
          if ( log != null ) {
            log.logDebug(
                BaseMessages.getString( PKG, "ServerUtils.Message.ScriptOutput" ) + "\n" + outAndErr.get( 0 ) );
            log.logDebug(
                BaseMessages.getString( PKG, "ServerUtils.Message.ScriptError" ) + "\n" + outAndErr.get( 1 ) );
          } else {
            System.err.println( "Script output:\n" + outAndErr.get( 0 ) );
            System.err.println( "\nScript error:\n" + outAndErr.get( 1 ) );
          }
        }

        if ( outAndErr.get( 1 ).contains( "Warning:" ) ) {
          // clear warnings - we really just want to know if there
          // are major errors
          outAndErr.set( 1, "" );
        }
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else if ( debug ) {
      outputCommandDebug( command, log );
    }

    return outAndErr;
  }

  /**
   * Send rows to python to be converted to a pandas data frame
   *
   * @param log          the log channel to use
   * @param inputStream  the input stream to read a response from
   * @param outputStream the output stream to talk to the server on
   * @throws KettleException if a problem occurs
   */
  protected static void sendRowsToPandasDataFrame( LogChannelInterface log, RowMetaInterface meta, List<Object[]> rows,
      String frameName, OutputStream outputStream, InputStream inputStream ) throws KettleException {
    ObjectMapper mapper = new ObjectMapper();

    boolean debug = log == null || log.isDebug();
    Map<String, Object> metaData = createMetadataMessage( frameName, meta );
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, ACCEPT_ROWS_COMMAND );
    command.put( NUM_ROWS_KEY, rows.size() );
    command.put( ROW_META_KEY, metaData );
    command.put( DEBUG_KEY, debug );

    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }

        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        // now write the CSV data
        if ( rows.size() > 0 ) {
          if ( log != null && debug ) {
            log.logDebug( "Sending CSV data..." );
          }
          bos = new ByteArrayOutputStream();
          BufferedWriter bw = new BufferedWriter( new OutputStreamWriter( bos ) );
          StringBuilder csv = rowsToCSV( meta, rows );
          bw.write( csv.toString() );
          bw.flush();
          bw.close();
          bytes = bos.toByteArray();
          writeDelimitedToOutputStream( bytes, outputStream );
        }

        String serverAck = receiveServerAck( inputStream );
        if ( serverAck != null ) {
          throw new KettleException(
              BaseMessages.getString( PKG, "ServerUtils.Error.TransferOfRowsFailed" ) + serverAck );
        }
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else if ( debug ) {
      outputCommandDebug( command, log );
    }
  }

  /**
   * Receive rows taken from a python pandas data frame
   *
   * @param log             the log channel to use
   * @param frameName       the name of the pandas frame to get
   * @param includeRowIndex true to include the frame row index as a field
   * @param inputStream     the input stream to read a response from
   * @param outputStream    the output stream to talk to the server on
   * @return the data frame converted to rows along with its associated row metadata
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static RowMetaAndRows receiveRowsFromPandasDataFrame(
      LogChannelInterface log, String frameName, boolean includeRowIndex, OutputStream outputStream,
      InputStream inputStream ) throws KettleException {

    boolean debug = log == null || log.isDebug();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, GET_FRAME_COMMAND );
    command.put( FRAME_NAME_KEY, frameName );
    command.put( FRAME_INCLUDE_ROW_INDEX, includeRowIndex );
    command.put( DEBUG_KEY, debug );

    RowMetaAndRows result = null;
    if ( inputStream != null && outputStream != null ) {
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        if ( debug ) {
          outputCommandDebug( command, log );
        }

        writeDelimitedToOutputStream( bytes, outputStream );
        String serverAck = receiveServerAck( inputStream );
        if ( serverAck != null ) {
          throw new KettleException( serverAck );
        }

        // read the header
        bytes = readDelimitedFromInputStream( inputStream );
        Map<String, Object> headerResponse = mapper.readValue( bytes, Map.class );
        if ( headerResponse == null ) {
          throw new KettleException( BaseMessages.getString( PKG, "ServerUtils.Error.HeaderMetadataMapIsNull" ) );
        }
        if ( headerResponse.get( RESPONSE_KEY ).toString().equals( ROW_META_KEY ) ) {
          if ( log != null ) {
            log.logDebug( BaseMessages
                .getString( PKG, "ServerUtils.Message.ReceivedMetadataResponse", headerResponse.get( NUM_ROWS_KEY ) ) );
          } else {
            System.err.println( BaseMessages
                .getString( PKG, "ServerUtils.Message.ReceivedMetadataResponse", headerResponse.get( NUM_ROWS_KEY ) ) );
          }
        } else {
          throw new KettleException( BaseMessages.getString( PKG, "ServerUtils.Error.UnknownResponseType" ) );
        }

        RowMetaInterface convertedMeta = jsonRowMetadataToRowMeta( frameName, headerResponse );
        int numRows = (Integer) headerResponse.get( NUM_ROWS_KEY );

        bytes = readDelimitedFromInputStream( inputStream );
        String csv = new String( bytes );
        result = csvToRows( csv, convertedMeta, numRows );
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else {
      outputCommandDebug( command, log );
    }

    return result;
  }

  /**
   * Convert csv data to rows
   *
   * @param csv        csv data in a String
   * @param kettleMeta metadata for the fields in the csv
   * @param numRows    the number of rows of data
   * @return rows of kettle data
   * @throws IOException if a problem occurs
   */
  protected static RowMetaAndRows csvToRows( String csv, RowMetaInterface kettleMeta, int numRows ) throws IOException {
    RowMetaAndRows rowMetaAndRows = new RowMetaAndRows();
    rowMetaAndRows.m_rowMeta = kettleMeta;
    rowMetaAndRows.m_rows = new Object[numRows][];
    int count = 0;
    // use a foreign line ending so that we can still have cr/lf in text cells
    for(String line: csv.split("#\\|\\|#")){
      // stash cr and lf so that they don't break the parser
      line = line.replace("\n", "<lf>").replace("\r", "<cr>");
      String[] parsed = PARSER.parseLine( line );

      Object[] row = new Object[kettleMeta.size()];
      for ( int i = 0; i < kettleMeta.size(); i++ ) {
        if ( parsed[i].equals( MISSING_VALUE ) ) {
          continue;
        }
        switch ( kettleMeta.getValueMeta( i ).getType() ) {
          case ValueMetaInterface.TYPE_NUMBER:
            try {
              row[i] = new Double( parsed[i] );
            } catch ( NumberFormatException ex ) {
              throw new IOException( ex );
            }
            break;
          case ValueMetaInterface.TYPE_BOOLEAN:
            row[i] = parsed[i].equalsIgnoreCase( "true" );
            break;
          case ValueMetaInterface.TYPE_DATE:
            try {
              row[i] = DATE_FORMAT.parse( parsed[i] );
            } catch ( ParseException ex ) {
              throw new IOException( ex );
            }
            break;
          default:
            // unpack stashed cr lf 
            row[i] = parsed[i].replace("<lf>", "\n").replace("<cr>", "\r");
        }
      }
      rowMetaAndRows.m_rows[count++] = row;
    }

    return rowMetaAndRows;
  }

  /**
   * Convert a json message containing field metadata to kettle row metadata
   *
   * @param frameName the name of the pandas frame that the metadata is for
   * @param jsonMeta  the json structure containing field metadata
   * @return the kettle row metadata
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static RowMetaInterface jsonRowMetadataToRowMeta( String frameName,
      Map<String, Object> jsonMeta ) throws KettleException {
    if ( !jsonMeta.get( FRAME_NAME_KEY ).equals( frameName ) ) {
      throw new KettleException( BaseMessages
          .getString( PKG, "ServerUtils.Error.IncorrectFrameSent", frameName, jsonMeta.get( FRAME_NAME_KEY ) ) );
    }
    List<Map<String, String>> fields = (List<Map<String, String>>) jsonMeta.get( FIELDS_KEY );

    RowMetaInterface rowMeta = new RowMeta();
    for ( Map<String, String> field : fields ) {
      String fieldName = field.get( FIELD_NAME_KEY );
      String fieldType = field.get( FIELD_TYPE_KEY );

      ValueMetaInterface vm;
      if ( fieldType.equals( FIELD_TYPE_NUMBER ) ) {
        //vm = new ValueMeta( fieldName, ValueMetaInterface.TYPE_NUMBER );
        vm = ValueMetaFactory.createValueMeta( fieldName, ValueMetaInterface.TYPE_NUMBER );
      } else if ( fieldType.equals( FIELD_TYPE_STRING ) ) {
        //vm = new ValueMeta( fieldName, ValueMetaInterface.TYPE_STRING );
        vm = ValueMetaFactory.createValueMeta( fieldName, ValueMetaInterface.TYPE_STRING );
      } else if ( fieldType.equals( FIELD_TYPE_DATE ) ) {
        // vm = new ValueMeta( fieldName, ValueMetaInterface.TYPE_DATE );
        vm = ValueMetaFactory.createValueMeta( fieldName, ValueMetaInterface.TYPE_DATE );
      } else if ( fieldType.equals( FIELD_TYPE_BOOLEAN ) ) {
        //vm = new ValueMeta( fieldName, ValueMetaInterface.TYPE_BOOLEAN );
        vm = ValueMetaFactory.createValueMeta( fieldName, ValueMetaInterface.TYPE_BOOLEAN );
      } else {
        throw new KettleException( BaseMessages.getString( PKG, "ServerUtils.Error.UnknownFieldType" ) + fieldType );
      }
      rowMeta.addValueMeta( vm );
    }

    return rowMeta;
  }

  /**
   * Convert rows of kettle data to a csv string
   *
   * @param meta the metadata of the rows
   * @param rows the rows of data
   * @return csv data stored in a StringBuilder
   */
  protected static StringBuilder rowsToCSV( RowMetaInterface meta, List<Object[]> rows ) {
    StringBuilder builder = new StringBuilder();
    // header row
    int i = 0;
    for ( ValueMetaInterface v : meta.getValueMetaList() ) {
      String name = quote( v.getName() );
      builder.append( i > 0 ? "," : "" ).append( name );
      i++;
    }
    builder.append( "\n" );
    for ( Object[] row : rows ) {
      for ( i = 0; i < row.length; i++ ) {
        String value;
        if ( row[i] == null || Const.isEmpty( row[i].toString() ) ) {
          value = "?";
        } else {
          switch ( meta.getValueMetaList().get( i ).getType() ) {
            case ValueMetaInterface.TYPE_NUMBER:
            case ValueMetaInterface.TYPE_INTEGER:
            case ValueMetaInterface.TYPE_BIGNUMBER:
              value = row[i].toString();
              break;
            case ValueMetaInterface.TYPE_DATE:
              value = "" + ( (Date) row[i] ).getTime();
              break;
            case ValueMetaInterface.TYPE_TIMESTAMP:
              value = "" + ( (Timestamp) row[i] ).getTime();
              break;
            case ValueMetaInterface.TYPE_BOOLEAN:
              value = "" + ( ( (Boolean) row[i] ) ? "1" : "0" );
              break;
            // TODO throw an exception for Serializable/Binary
            default:
              value = quote( row[i].toString() );
          }
          builder.append( i > 0 ? "," : "" ).append( value );
        }
      }
      builder.append( "\n" );
    }

    return builder;
  }

  /**
   * Quote the supplied string
   *
   * @param string the string to quote
   * @return a quoted string
   */
  protected static String quote( String string ) {
    boolean quote = false;

    // backquote the following characters
    if ( ( string.indexOf( '\n' ) != -1 ) || ( string.indexOf( '\r' ) != -1 ) || ( string.indexOf( '\'' ) != -1 ) || (
        string.indexOf( '"' ) != -1 ) || ( string.indexOf( '\\' ) != -1 ) || ( string.indexOf( '\t' ) != -1 ) || (
        string.indexOf( '%' ) != -1 ) || ( string.indexOf( '\u001E' ) != -1 ) ) {
      string = backQuoteChars( string );
      quote = true;
    }

    // Enclose the string in 's if the string contains a recently added
    // backquote or contains one of the following characters.
    if ( ( quote == true ) || ( string.indexOf( '{' ) != -1 ) || ( string.indexOf( '}' ) != -1 ) || (
        string.indexOf( ',' ) != -1 ) || ( string.equals( "?" ) ) || ( string.indexOf( ' ' ) != -1 ) || ( string
        .equals( "" ) ) ) {
      string = ( "'".concat( string ) ).concat( "'" );
    }

    return string;
  }

  /**
   * Converts carriage returns and new lines in a string into \r and \n.
   * Backquotes the following characters: ` " \ \t and %
   *
   * @param string the string
   * @return the converted string
   */
  protected static String backQuoteChars( String string ) {

    int index;
    StringBuffer newStringBuffer;

    // replace each of the following characters with the backquoted version
    char[] charsFind = { '\\', '\'', '\t', '\n', '\r', '"', '%', '\u001E' };
    String[] charsReplace = { "\\\\", "\\'", "\\t", "\\n", "\\r", "\\\"", "\\%", "\\u001E" };
    for ( int i = 0; i < charsFind.length; i++ ) {
      if ( string.indexOf( charsFind[i] ) != -1 ) {
        newStringBuffer = new StringBuffer();
        while ( ( index = string.indexOf( charsFind[i] ) ) != -1 ) {
          if ( index > 0 ) {
            newStringBuffer.append( string.substring( 0, index ) );
          }
          newStringBuffer.append( charsReplace[i] );
          if ( ( index + 1 ) < string.length() ) {
            string = string.substring( index + 1 );
          } else {
            string = "";
          }
        }
        newStringBuffer.append( string );
        string = newStringBuffer.toString();
      }
    }

    return string;
  }

  /**
   * Write length delimited data to the output stream
   *
   * @param bytes        the bytes to write
   * @param outputStream the output stream to write to
   * @throws IOException if a problem occurs
   */
  protected static void writeDelimitedToOutputStream( byte[] bytes, OutputStream outputStream ) throws IOException {

    // write the message length as a fixed size integer
    outputStream.write( ByteBuffer.allocate( 4 ).putInt( bytes.length ).array() );

    // write the message itself
    outputStream.write( bytes );
  }

  /**
   * Read length delimited data from the input stream
   *
   * @param inputStream the input stream to read from
   * @return the bytes read
   * @throws IOException if a problem occurs
   */
  protected static byte[] readDelimitedFromInputStream( InputStream inputStream ) throws IOException {
    byte[] sizeBytes = new byte[4];
    int numRead = inputStream.read( sizeBytes, 0, 4 );
    if ( numRead < 4 ) {
      throw new IOException( BaseMessages.getString( PKG, "ServerUtils.Error.FailedToReadMessageSize" ) );
    }

    int messageLength = ByteBuffer.wrap( sizeBytes ).getInt();
    byte[] messageData = new byte[messageLength];
    // for (numRead = 0; numRead < messageLength; numRead +=
    // inputStream.read(messageData, numRead, messageLength - numRead));
    for ( numRead = 0; numRead < messageLength; ) {
      int currentNumRead = inputStream.read( messageData, numRead, messageLength - numRead );
      if ( currentNumRead < 0 ) {
        throw new IOException( BaseMessages.getString( PKG, "ServerUtils.Error.UnexpectedEndOfStream" ) );
      }
      numRead += currentNumRead;
    }

    return messageData;
  }

  /**
   * Receive a simple ack from the server. Returns a non-null string if the ack
   * received contains an error message
   *
   * @param inputStream the input stream to read the ack from
   * @return a non-null string if there was an error returned by the server
   * @throws IOException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static String receiveServerAck( InputStream inputStream )
      throws IOException {
    byte[] bytes = readDelimitedFromInputStream( inputStream );
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> ack = mapper.readValue( bytes, Map.class );

    String response = ack.get( RESPONSE_KEY ).toString();
    if ( response.equals( OK_KEY ) ) {
      return null;
    }

    return ack.get( ERROR_MESSAGE_KEY ).toString();
  }

  /**
   * Receives a PID ack from the server
   *
   * @param inputStream the input stream to read from
   * @return the process ID of the server
   * @throws IOException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static int receiveServerPIDAck( InputStream inputStream )
      throws IOException {
    byte[] bytes = readDelimitedFromInputStream( inputStream );
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> ack = mapper.readValue( bytes, Map.class );

    String response = ack.get( RESPONSE_KEY ).toString();
    if ( response.equals( PID_RESPONSE_KEY ) ) {
      return (Integer) ack.get( "pid" );
    } else {
      throw new IOException( BaseMessages.getString( PKG, "ServerUtils.Error.NoPidResponse" ) );
    }
  }

  /**
   * Std out and err are redirected to StringIO objects in the server. This
   * method retrieves the values of those buffers.
   *
   * @param outputStream the output stream to talk to the server on
   * @param inputStream  the input stream to receive server responses from
   * @param log          optional log
   * @return the std out and err strings as a two element list
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static List<String> receiveDebugBuffer( OutputStream outputStream,
      InputStream inputStream, LogChannelInterface log ) throws KettleException {
    List<String> stdOutStdErr = new ArrayList<String>();

    boolean debug = log == null || log.isDebug();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( "command", "get_debug_buffer" );

    if ( inputStream != null && outputStream != null ) {
      try {
        if ( debug ) {
          outputCommandDebug( command, log );
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        bytes = readDelimitedFromInputStream( inputStream );
        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( "response" ).toString().equals( "ok" ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }
        Object stOut = ack.get( "std_out" );
        stdOutStdErr.add( stOut != null ? stOut.toString() : "" );
        Object stdErr = ack.get( "std_err" );
        stdOutStdErr.add( stdErr != null ? stdErr.toString() : "" );
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else if ( debug ) {
      outputCommandDebug( command, log );
    }

    return stdOutStdErr;
  }

  /**
   * Get the type of a variable in python
   *
   * @param varName      the name of the variable to check
   * @param outputStream the output stream to talk to the server on
   * @param inputStream  the input stream to receive server responses from
   * @param log          an optional log
   * @return the type of the variable in python
   * @throws KettleException if a problem occurs
   */
  @SuppressWarnings( "unchecked" ) protected static PythonVariableType getPythonVariableType( String varName,
      OutputStream outputStream, InputStream inputStream, LogChannelInterface log ) throws KettleException {

    boolean debug = log == null || log.isDebug();
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( COMMAND_KEY, GET_VARIABLE_TYPE_KEY );
    command.put( VARIABLE_NAME_KEY, varName );
    command.put( DEBUG_KEY, debug );
    if ( inputStream != null && outputStream != null ) {
      try {
        if ( debug ) {
          outputCommandDebug( command, log );
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        mapper.writeValue( bos, command );
        byte[] bytes = bos.toByteArray();

        // write the command
        writeDelimitedToOutputStream( bytes, outputStream );

        bytes = readDelimitedFromInputStream( inputStream );
        Map<String, Object> ack = mapper.readValue( bytes, Map.class );
        if ( !ack.get( RESPONSE_KEY ).toString().equals( OK_KEY ) ) {
          // fatal error
          throw new KettleException( ack.get( ERROR_MESSAGE_KEY ).toString() );
        }

        if ( !ack.get( VARIABLE_NAME_KEY ).toString().equals( varName ) ) {
          throw new KettleException( "Server sent back a response for a different " + "variable!" );
        }

        String varType = ack.get( VARIABLE_TYPE_RESPONSE_KEY ).toString();
        PythonSession.PythonVariableType pvt = PythonSession.PythonVariableType.Unknown;
        for ( PythonSession.PythonVariableType t : PythonSession.PythonVariableType.values() ) {
          if ( t.toString().toLowerCase().equals( varType ) ) {
            pvt = t;
            break;
          }
        }
        return pvt;
      } catch ( IOException ex ) {
        throw new KettleException( ex );
      }
    } else {
      outputCommandDebug( command, log );
    }

    return PythonSession.PythonVariableType.Unknown;
  }

  /**
   * Send a shutdown command to the micro server
   *
   * @param outputStream the output stream to write the command to
   * @throws KettleException if a problem occurs
   */
  protected static void sendServerShutdown( OutputStream outputStream ) throws KettleException {
    Map<String, Object> command = new HashMap<String, Object>();
    command.put( "command", "shutdown" );
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectMapper mapper = new ObjectMapper();
      mapper.writeValue( bos, command );
      byte[] bytes = bos.toByteArray();

      // write the command
      writeDelimitedToOutputStream( bytes, outputStream );
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }

  /**
   * Prints out a json command for debugging purposes
   *
   * @param command the command to print out
   * @param log     optional log
   */
  protected static void outputCommandDebug( Map<String, Object> command, LogChannelInterface log )
      throws KettleException {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter sw = new StringWriter();
    try {
      mapper.writeValue( sw, command );
      String serialized = sw.toString();
      if ( log != null ) {
        log.logDebug( "Sending command:\n" + serialized );
      } else {
        System.err.println( "Sending command: " );
        System.err.println( serialized );
      }
    } catch ( IOException ex ) {
      throw new KettleException( ex );
    }
  }
}
