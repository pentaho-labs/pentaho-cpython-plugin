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

import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;

import java.util.Random;

public class CPythonRowMetaInterfaceValidator implements FieldLoadSaveValidator<RowMetaInterface> {

  private final Random random = new Random();

  @Override public RowMetaInterface getTestObject() {
    int size = random.nextInt( 10 ) + 1;
    RowMetaInterface result = new RowMeta();

    for ( int i = 0; i < size; i++ ) {
      try {
        ValueMetaInterface vm =
            ValueMetaFactory.createValueMeta( "field" + i,
                i % 2 == 0 ? ValueMetaInterface.TYPE_STRING : ValueMetaInterface.TYPE_NUMBER );
        result.addValueMeta( vm );
      } catch ( KettlePluginException e ) {
        throw new RuntimeException( e );
      }
    }

    return result;
  }

  @Override public boolean validateTestObject( RowMetaInterface testObject, Object other ) {
    if ( other == null || !( other instanceof RowMetaInterface ) ) {
      return false;
    }
    RowMetaInterface otherRow = (RowMetaInterface) other;
    if ( testObject.size() != otherRow.size() ) {
      return false;
    }
    for ( int i = 0; i < testObject.size(); i++ ) {
      ValueMetaInterface testVmi = testObject.getValueMeta( i );
      ValueMetaInterface otherVmi = otherRow.getValueMeta( i );
      if ( !testVmi.getName().equals( otherVmi.getName() ) ) {
        return false;
      }
      if ( !testVmi.getTypeDesc().equals( otherVmi.getTypeDesc() ) ) {
        return false;
      }
    }
    return true;
  }
}
