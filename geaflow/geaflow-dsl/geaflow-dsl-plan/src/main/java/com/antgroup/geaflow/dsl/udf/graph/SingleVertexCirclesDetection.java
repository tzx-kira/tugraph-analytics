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

package com.antgroup.geaflow.dsl.udf.graph;

import com.antgroup.geaflow.common.tuple.Tuple;
import com.antgroup.geaflow.common.type.primitive.BooleanType;
import com.antgroup.geaflow.common.type.primitive.LongType;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmRuntimeContext;
import com.antgroup.geaflow.dsl.common.algo.AlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.algo.IncrementalAlgorithmUserFunction;
import com.antgroup.geaflow.dsl.common.data.Row;
import com.antgroup.geaflow.dsl.common.data.RowEdge;
import com.antgroup.geaflow.dsl.common.data.RowVertex;
import com.antgroup.geaflow.dsl.common.data.impl.ObjectRow;
import com.antgroup.geaflow.dsl.common.function.Description;
import com.antgroup.geaflow.dsl.common.types.GraphSchema;
import com.antgroup.geaflow.dsl.common.types.StructType;
import com.antgroup.geaflow.dsl.common.types.TableField;
import com.antgroup.geaflow.dsl.common.util.TypeCastUtil;
import com.antgroup.geaflow.model.graph.edge.EdgeDirection;

import java.util.*;

// 单顶点环检测算法
@Description(name = "single_vertex_circles_detection", description = "Detects circles starting from a single vertex")
public class SingleVertexCirclesDetection implements AlgorithmUserFunction<Long, Tuple<Long, Integer>>{
    private AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context;
    private static final int MAX_D = 10;  // 最大搜索深度（防止无限循环）
    private Set<Tuple<Long, Integer>> circleResults = new HashSet<>();
    private Set<Long> verticesInCircle = new HashSet<>(); // 存储所有检测到环的顶点ID
    // 初始化方法
    @Override
    public void init(AlgorithmRuntimeContext<Long, Tuple<Long, Integer>> context,Object[] params) {
        this.context = context;
    }

    // 顶点处理逻辑
    @Override
    public void process(RowVertex vertex, Optional<Row> updatedValues, Iterator<Tuple<Long, Integer>> messages) {
        Long selfId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        long iteration = context.getCurrentIterationId();

        // 第一轮迭代：从源顶点启动遍历
        if (iteration == 1L) {
            // 构建初始消息（源顶点ID + 当前路径长度1）
            Tuple<Long, Integer> msg = Tuple.of(selfId, 1);
            // 遍历所有出边邻居
            for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(), Long.class);
                // 向邻居发送消息
                context.sendMessage(targetId, msg);
            }
        } else {
            // 后续迭代：处理接收到的消息
            while (messages.hasNext()) {
                Tuple<Long, Integer> msg = messages.next();
                Long startId = msg.getF0(); // 路径起始顶点ID
                int pathLen = msg.getF1(); // 当前路径长度
                // 环检测条件：当前顶点等于路径起点
                if (Objects.equals(selfId, startId)) {
                    verticesInCircle.add(selfId);
                    circleResults.add(Tuple.of(selfId, pathLen));
                    continue;
                }
                // 路径长度超过阈值则停止传播
                if (pathLen >= MAX_D) continue;
                // 构建新消息（保持原起点，路径长度+1）
                Tuple<Long, Integer> newMsg = Tuple.of(startId, pathLen + 1);
                // 继续向所有出边邻居传播
                for (RowEdge edge : context.loadEdges(EdgeDirection.OUT)) {
                    Long targetId = (Long) TypeCastUtil.cast(edge.getTargetId(),Long.class);
                    context.sendMessage(targetId, newMsg);
                }
            }

        }
    }

    // 定义输出数据结构
    @Override
    public StructType getOutputType(GraphSchema graphSchema) {
        return new StructType(
            new TableField("vertex_id", graphSchema.getIdType(), false), // 顶点ID字段
            new TableField("circle_length", LongType.INSTANCE, false)   // 环长度字段
        );
    }

    // 算法结束处理（结果输出）
    @Override
    public void finish(RowVertex vertex, Optional<Row> updatedValues) {
        Long vertexId = (Long) TypeCastUtil.cast(vertex.getId(), Long.class);
        // 仅输出在环中的顶点
        if (verticesInCircle.contains(vertexId)) {
            // 找出该顶点参与的最小环长度
            int minCircleLength = Integer.MAX_VALUE;
            for (Tuple<Long, Integer> result : circleResults) {
                if (Objects.equals(result.getF0(), vertexId)) {
                    minCircleLength = Math.min(minCircleLength, result.getF1());
                }
            }
            // 输出结果（顶点ID + 最小环长度）
            if (minCircleLength != Integer.MAX_VALUE) {
                context.take(ObjectRow.create(vertexId, minCircleLength));
            }
        }
    }

}
