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

/**
 * For session locking.
 *
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class SessionMutex {
  private boolean m_verbose;

  /**
   * defines the current mutex state
   */
  private boolean m_locked;

  /**
   * thread that m_locked this mutex
   */
  private Thread m_lockedBy;

  public SessionMutex() {
  }

  public SessionMutex( boolean verbose ) {
    m_verbose = verbose;
  }

  private synchronized void lock() {
    while ( m_locked ) {
      if ( m_lockedBy == Thread.currentThread() ) {
        System.err.println( "INFO: Mutex detected a deadlock! The application is likely to hang indefinitely!" );
      }

      if ( m_verbose ) {
        System.out.println( "INFO: " + toString() + " is m_locked by " + m_lockedBy + ", but " + Thread.currentThread()
            + " waits for release" );
      }
      try {
        wait();
      } catch ( InterruptedException e ) {
        if ( m_verbose )
          System.out.println( "INFO: " + toString() + " caught InterruptedException" );
      }
    }
    m_locked = true;
    m_lockedBy = Thread.currentThread();
    if ( m_verbose ) {
      System.out.println( "INFO: " + toString() + " m_locked by " + m_lockedBy );
    }
  }

  public synchronized boolean safeLock() {
    if ( m_locked && m_lockedBy == Thread.currentThread() ) {
      if ( m_verbose ) {
        System.out.println( "INFO: " + toString() + " unable to provide safe lock for " + Thread.currentThread() );
      }
      return false;
    }
    lock();
    return true;
  }

  public synchronized void unlock() {
    if ( m_locked && m_lockedBy != Thread.currentThread() ) {
      System.err.println( "WARNING: Mutex was unlocked by other thread" );
    }
    m_locked = false;
    if ( m_verbose ) {
      System.out.println( "INFO: " + toString() + " unlocked by " + Thread.currentThread() );
    }

    // notify just 1 in case more of them are waiting
    notify();
  }
}
