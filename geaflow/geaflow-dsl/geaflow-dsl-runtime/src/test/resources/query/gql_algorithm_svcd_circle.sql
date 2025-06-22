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
 
-- 创建结果表
CREATE TABLE result_tb (
    vid bigint,
    circle_length bigint
) WITH (
    type='file',
    geaflow.dsl.file.path='${target}'
);
-- 使用自定义的mygraph_01图
USE GRAPH mygraph_01;
-- 调用算法并插入结果
INSERT INTO result_tb
CALL single_vertex_circles_detection() YIELD (vertex_id, circle_length)
RETURN vertex_id, circle_length;