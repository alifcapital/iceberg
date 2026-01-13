/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.actions;

/** Utility methods for UUID detection and handling. */
final class UuidUtil {

  private UuidUtil() {}

  /**
   * Detects UUID format in a string value.
   *
   * <p>UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx Truncated values are supported (minimum 9
   * chars: 8 hex + dash)
   */
  static boolean looksLikeUuid(String value) {
    if (value == null || value.length() < 9) {
      return false;
    }

    // Check for dash at position 8 (UUID format: xxxxxxxx-xxxx-...)
    if (value.charAt(8) != '-') {
      return false;
    }

    // Check that first 8 characters are hex
    for (int i = 0; i < 8; i++) {
      char c = value.charAt(i);
      if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
        return false;
      }
    }

    return true;
  }
}
