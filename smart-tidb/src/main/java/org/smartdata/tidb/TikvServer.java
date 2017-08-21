/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.tidb;

import com.sun.jna.Library;
import com.sun.jna.Native;

public class TikvServer implements Runnable {
    String args;
    public interface Tikv extends Library {
        void startServer(String args);
    }
    public TikvServer(String args){
        this.args=args;
    }
    public void run(){
        Tikv kv=null;
        try {
            kv= (Tikv) Native.loadLibrary("libtikv.so",Tikv.class);
        }
        catch (UnsatisfiedLinkError ex){
            ex.printStackTrace();
            System.exit(1);
        }

        StringBuffer strbuffer=new StringBuffer();
        strbuffer.append("TiKV");  //@ App::new("TiKV") in start.rs, "TiKV" is the flag name used for parsing
        strbuffer.append(" ");
        strbuffer.append(args);

        System.out.println("starting TiKV..");
        kv.startServer(strbuffer.toString());
    }
}
