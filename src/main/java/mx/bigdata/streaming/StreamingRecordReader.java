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

package mx.bigdata.streaming;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;


public final class StreamingRecordReader<T> implements Iterable<T>, Closeable {
  
  private final BufferedReader reader;
  
  private final RecordBuilder<T> builder;

  public static <T> StreamingRecordReader<T> newReader(Reader reader, RecordBuilder<T> builder) {
    return new StreamingRecordReader<T>(reader, builder);
  }
  
  private StreamingRecordReader(Reader reader, RecordBuilder<T> builder) {
    this.reader = new BufferedReader(reader);
    this.builder = builder;
  }

  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      
      private String line;
      
      @Override
      public boolean hasNext() {
        try {
          return (line = reader.readLine()) != null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public T next() {
        return builder.build(line);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
      
    };
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
  
}
