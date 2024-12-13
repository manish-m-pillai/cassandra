/*
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

package org.apache.cassandra.distributed.test;

import java.io.IOException;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;


import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.net.UnknownHostException;
import java.util.stream.Collectors;


public class NodeToolStatusTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static IInvokableInstance NODE_1;
    private static IInvokableInstance NODE_2;
    private static IInvokableInstance NODE_3;

    @BeforeClass
    public static void before() throws IOException
    {
        CLUSTER = init(Cluster.build().withNodes(3).start());
        NODE_1 = CLUSTER.get(1);
        NODE_2 = CLUSTER.get(2);
        NODE_3 = CLUSTER.get(3);
    }

    @AfterClass
    public static void after()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    // Test Commands to validate the commands are working as expected.
    @Test
    public void testCommands()
    {
        assertEquals(0, NODE_1.nodetool("status"));
        assertEquals(0, NODE_1.nodetool("status", "-s", "ip"));
        assertEquals(0, NODE_1.nodetool("status", "-s", "ip", "-o", "asc"));
        assertEquals(0, NODE_1.nodetool("status", "-s", "ip", "-o", "desc"));
        assertEquals(0, NODE_1.nodetool("status", "--sort", "ip", "--order", "desc"));
        assertEquals(1, NODE_1.nodetool("status", "--sort", "not_an_option"));
        assertEquals(1, NODE_1.nodetool("status", "--sort", "ip", "-o", "not_an_order"));
    }

    // Test Cases for Ip
    @Test
    public void testSortByIpDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "ip");
        result.asserts().success();
        String output = result.getStdout();

        // Extract IP addresses and ensure they are sorted in ascending order (default order)
        List<String> ipAddresses = extractColumn(output, 1);
        List<InetAddressAndPort> ipList = ipAddresses.stream()
                .map(this::parseInetAddress)
                .collect(Collectors.toList());
        List<InetAddressAndPort> sortedIpList = new ArrayList<>(ipList);
        Collections.sort(sortedIpList);
        assertEquals("IP addresses are not sorted in ascending order", sortedIpList, ipList);
    }

    @Test
    public void testSortByIpAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "ip", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract IP addresses and ensure they are sorted in ascending order
        List<String> ipAddresses = extractColumn(output, 1);
        List<InetAddressAndPort> ipList = ipAddresses.stream()
                .map(this::parseInetAddress)
                .collect(Collectors.toList());
        List<InetAddressAndPort> sortedIpList = new ArrayList<>(ipList);
        Collections.sort(sortedIpList);
        assertEquals("IP addresses are not sorted in ascending order", sortedIpList, ipList);
    }

    @Test
    public void testSortByIpDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "ip", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract IP addresses and ensure they are sorted in descending order
        List<String> ipAddresses = extractColumn(output, 1);
        List<InetAddressAndPort> ipList = ipAddresses.stream()
                .map(this::parseInetAddress)
                .collect(Collectors.toList());
        List<InetAddressAndPort> sortedIpList = new ArrayList<>(ipList);
        Collections.sort(sortedIpList, Collections.reverseOrder());
        assertEquals("IP addresses are not sorted in descending order", sortedIpList, ipList);
    }

    // Test Cases for Load
    @Test
    public void testSortByLoadDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "load");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the load values and handle '?' placement
        List<String> loads = extractColumn(output, 2);
        List<Long> loadList = loads.stream()
                .map(load -> {
                    // If load contains '?', assign it a value that will be placed last
                    if (load.contains("?"))
                        return Long.MIN_VALUE;
                    return FileUtils.parseFileSize(load);
                })
                .collect(Collectors.toList());

        // Sort the load list in descending order (default order)
        List<Long> sortedLoadList = new ArrayList<>(loadList);
        Collections.sort(sortedLoadList, Collections.reverseOrder());

        // Assert that the load values are sorted in descending order, considering '?' as last
        assertEquals("Load values are not sorted in descending order", sortedLoadList, loadList);
    }

    @Test
    public void testSortByLoadAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "load", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the load values and handle '?' placement
        List<String> loads = extractColumn(output, 2);
        List<Long> loadList = loads.stream()
                .map(load -> {
                    // If load contains '?', assign it a value that will be placed first
                    if (load.contains("?"))
                        return Long.MIN_VALUE;
                    return FileUtils.parseFileSize(load);
                })
                .collect(Collectors.toList());

        // Sort the load list in ascending order
        List<Long> sortedLoadList = new ArrayList<>(loadList);
        Collections.sort(sortedLoadList);

        // Assert that the load values are sorted in ascending order, considering '?' as first
        assertEquals("Load values are not sorted in ascending order", sortedLoadList, loadList);
    }

    @Test
    public void testSortByLoadDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "load", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the load values and handle '?' placement
        List<String> loads = extractColumn(output, 2);
        List<Long> loadList = loads.stream()
                .map(load -> {
                    // If load contains '?', assign it a value that will be placed last
                    if (load.contains("?"))
                        return Long.MIN_VALUE;
                    return FileUtils.parseFileSize(load);
                })
                .collect(Collectors.toList());

        // Sort the load list in descending order
        List<Long> sortedLoadList = new ArrayList<>(loadList);
        Collections.sort(sortedLoadList, Collections.reverseOrder());

        // Assert that the load values are sorted in descending order, considering '?' as last
        assertEquals("Load values are not sorted in descending order", sortedLoadList, loadList);
    }

    // Test cases for owns
    @Test
    public void testSortByOwnsDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "owns");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the ownership percentage and ensure they are sorted in descending order (default order)
        List<String> owns = extractColumn(output, 3);
        List<Double> ownsList = owns.stream()
                .map(s -> Double.parseDouble(s.replace("%", "")))
                .collect(Collectors.toList());
        List<Double> sortedOwnsList = new ArrayList<>(ownsList);
        Collections.sort(sortedOwnsList, Collections.reverseOrder());
        assertEquals("Ownership percentages are not sorted in descending order", sortedOwnsList, ownsList);
    }

    @Test
    public void testSortByOwnsAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "owns", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the ownership percentage and ensure they are sorted in ascending order
        List<String> owns = extractColumn(output, 3);
        List<Double> ownsList = owns.stream()
                .map(s -> Double.parseDouble(s.replace("%", "")))
                .collect(Collectors.toList());
        List<Double> sortedOwnsList = new ArrayList<>(ownsList);
        Collections.sort(sortedOwnsList);
        assertEquals("Ownership percentages are not sorted in ascending order", sortedOwnsList, ownsList);
    }

    @Test
    public void testSortByOwnsDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "owns", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract the ownership percentage and ensure they are sorted in descending order
        List<String> owns = extractColumn(output, 3);
        List<Double> ownsList = owns.stream()
                .map(s -> Double.parseDouble(s.replace("%", "")))
                .collect(Collectors.toList());
        List<Double> sortedOwnsList = new ArrayList<>(ownsList);
        Collections.sort(sortedOwnsList, Collections.reverseOrder());
        assertEquals("Ownership percentages are not sorted in descending order", sortedOwnsList, ownsList);
    }

    // Test cases for State
    @Test
    public void testSortByStateDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "state");
        result.asserts().success();
        String output = result.getStdout();

        // Extract state values and ensure they are sorted in descending order (default order)
        List<String> states = extractColumn(output, 0);
        List<String> sortedStates = new ArrayList<>(states);
        Collections.sort(sortedStates, Collections.reverseOrder());
        assertEquals("States are not sorted in descending order", sortedStates, states);
    }

    @Test
    public void testSortByStateAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "state", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract state values and ensure they are sorted in ascending order
        List<String> states = extractColumn(output, 0);
        List<String> sortedStates = new ArrayList<>(states);
        Collections.sort(sortedStates);
        assertEquals("States are not sorted in ascending order", sortedStates, states);
    }

    @Test
    public void testSortByStateDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "state", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract state values and ensure they are sorted in descending order
        List<String> states = extractColumn(output, 0);
        List<String> sortedStates = new ArrayList<>(states);
        Collections.sort(sortedStates, Collections.reverseOrder());
        assertEquals("States are not sorted in descending order", sortedStates, states);
    }

    // Test cases for Host
    @Test
    public void testSortByHostDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "id");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in ascending order (default order)
        List<String> racks = extractColumn(output, 5);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks);
        assertEquals("Rack values are not sorted in ascending order", sortedRacks, racks);
    }

    @Test
    public void testSortByHostAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "id", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in ascending order
        List<String> racks = extractColumn(output, 5);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks);
        assertEquals("Rack values are not sorted in ascending order", sortedRacks, racks);
    }

    @Test
    public void testSortByHostDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "id", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in descending order
        List<String> racks = extractColumn(output, 5);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks, Collections.reverseOrder());
        assertEquals("Rack values are not sorted in descending order", sortedRacks, racks);
    }

    // Test cases for Rack
    @Test
    public void testSortByRackDefault()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "rack");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in ascending order (default order)
        List<String> racks = extractColumn(output, 6);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks);
        assertEquals("Rack values are not sorted in ascending order", sortedRacks, racks);
    }

    @Test
    public void testSortByRackAsc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "rack", "-o", "asc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in ascending order
        List<String> racks = extractColumn(output, 6);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks);
        assertEquals("Rack values are not sorted in ascending order", sortedRacks, racks);
    }

    @Test
    public void testSortByRackDesc()
    {
        NodeToolResult result = NODE_1.nodetoolResult("status", "-s", "rack", "-o", "desc");
        result.asserts().success();
        String output = result.getStdout();

        // Extract rack values and ensure they are sorted in descending order
        List<String> racks = extractColumn(output, 6);
        List<String> sortedRacks = new ArrayList<>(racks);
        Collections.sort(sortedRacks, Collections.reverseOrder());
        assertEquals("Rack values are not sorted in descending order", sortedRacks, racks);
    }

    // Helper Methods
    private InetAddressAndPort parseInetAddress(String ip)
    {
        try
        {
            return InetAddressAndPort.getByName(ip);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException("Invalid IP address", e);
        }
    }

    private List<String> extractColumn(String output, int columnIndex)
    {
        List<String> columnValues = new ArrayList<>();
        String[] lines = output.split("\n");

        // Skip the first five lines as headers
        int skippedLines = 0;
        for (String line : lines)
        {
            if (line.trim().isEmpty())
                continue; // Skip separator lines and empty lines

            // Skip the first five lines as they are headers
            if (skippedLines < 5)
            {
                skippedLines++;
                continue;
            }

            // Use regular expression to extract columns
            // Pattern will match any column with possible varying whitespace
            String[] columns = line.trim().split("\\s{2,}");  // Split on 2 or more spaces

            // Check if the line has enough columns (avoid index out of bounds errors)
            if (columns.length > columnIndex)
                columnValues.add(columns[columnIndex].trim());
        }

        return columnValues;
    }
}