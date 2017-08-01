/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.smartdata.interpreter.impl;

import org.smartdata.interpreter.SmartInterpreter;
import org.smartdata.model.RuleState;
import org.smartdata.server.SmartEngine;

import java.io.IOException;

/**
 * Created by zhiqiangx on 17-7-31.
 */
public class RuleInterpreter extends SmartInterpreter {

    public RuleInterpreter(SmartEngine smartEngine) {
        super(smartEngine);
    }

    @Override
    public String excute(String cmd) throws IOException {
        long t =getSmartEngine().getRuleManager().submitRule(cmd, RuleState.DISABLED);
        return "Success to add rule : " + t;
    }
}
