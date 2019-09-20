/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.s3.sink.internal.utils;

/**
 * Enumeration for mapper types.
 */
public enum MapperTypes {
    Binary("binary", "bin"),
    Avro("avro", "bin"),
    CSV("csv", "csv"),
    JSON("json", "json"),
    XML("xml", "xml"),
    Text("text", "txt");

    private final String mapName;
    private final String extension;

    MapperTypes(String mapName, String extension) {
        this.mapName = mapName;
        this.extension = extension;
    }

    public static MapperTypes forName(String mapperName) {
        for (MapperTypes mapperType : MapperTypes.values()) {
            if (mapperType.getMapName().equalsIgnoreCase(mapperName)) {
                return mapperType;
            }
        }
        return MapperTypes.Text;
    }

    public String getMapName() {
        return mapName;
    }

    public String getExtension() {
        return extension;
    }
}
