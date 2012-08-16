/*
 *  Copyright 2012 BigData Mx
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package mx.bigdata.streaming.impl;

import mx.bigdata.streaming.RecordBuilder;

public class SplitBuilder implements RecordBuilder<String[]> {

  private final String sep;
  
  public SplitBuilder(String sep) {
    this.sep = sep;
  }
  
  public String[] build(String line) {
    return line.split(sep);
  }
}
