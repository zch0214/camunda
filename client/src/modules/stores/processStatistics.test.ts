/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import {waitFor} from '@testing-library/react';
import {mockFetchProcessInstances} from 'modules/mocks/api/processInstances/fetchProcessInstances';
import {mockProcessInstances} from 'modules/testUtils';
import {mockProcessStatistics} from 'modules/mocks/mockProcessStatistics';
import {processInstancesStore} from './processInstances';
import {processStatisticsStore} from './processStatistics';
import {mockFetchProcessInstancesStatistics} from 'modules/mocks/api/processInstances/fetchProcessInstancesStatistics';

describe('stores/processStatistics', () => {
  afterEach(() => {
    processStatisticsStore.reset();
    processInstancesStore.reset();
  });

  it('should fetch process statistics', async () => {
    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);

    expect(processStatisticsStore.state.statistics).toEqual([]);

    processStatisticsStore.fetchProcessStatistics();
    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual(
        mockProcessStatistics,
      ),
    );
  });

  it('should get flowNodeStates', async () => {
    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);

    processStatisticsStore.fetchProcessStatistics();

    await waitFor(() =>
      expect(processStatisticsStore.flowNodeStates).toEqual([
        {
          count: 1,
          flowNodeId: 'userTask',
          flowNodeState: 'active',
        },
        {
          count: 2,
          flowNodeId: 'userTask',
          flowNodeState: 'canceled',
        },
        {
          count: 3,
          flowNodeId: 'EndEvent_0crvjrk',
          flowNodeState: 'incidents',
        },
        {
          count: 4,
          flowNodeId: 'EndEvent_0crvjrk',
          flowNodeState: 'canceled',
        },
      ]),
    );
  });

  it('should get overlaysData', async () => {
    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);

    processStatisticsStore.fetchProcessStatistics();

    await waitFor(() =>
      expect(processStatisticsStore.overlaysData).toEqual([
        {
          flowNodeId: 'userTask',
          payload: {
            count: 1,
            flowNodeState: 'active',
          },
          position: {
            bottom: 9,
            left: 0,
          },
          type: 'statistics-active',
        },
        {
          flowNodeId: 'userTask',
          payload: {
            count: 2,
            flowNodeState: 'canceled',
          },
          position: {
            left: 0,
            top: -16,
          },
          type: 'statistics-canceled',
        },
        {
          flowNodeId: 'EndEvent_0crvjrk',
          payload: {
            count: 3,
            flowNodeState: 'incidents',
          },
          position: {
            bottom: 9,
            right: 0,
          },
          type: 'statistics-incidents',
        },
        {
          flowNodeId: 'EndEvent_0crvjrk',
          payload: {
            count: 4,
            flowNodeState: 'canceled',
          },
          position: {
            left: 0,
            top: -16,
          },
          type: 'statistics-canceled',
        },
      ]),
    );
  });

  it('should fetch process statistics depending on completed operations', async () => {
    const processInstance = mockProcessInstances.processInstances[0]!;
    mockFetchProcessInstances().withSuccess({
      processInstances: [{...processInstance, hasActiveOperation: true}],
      totalCount: 1,
    });

    processStatisticsStore.init();
    processInstancesStore.init();
    processInstancesStore.fetchProcessInstancesFromFilters();

    await waitFor(() =>
      expect(processInstancesStore.state.status).toBe('fetched'),
    );

    expect(processStatisticsStore.state.statistics).toEqual([]);

    mockFetchProcessInstances().withSuccess({
      processInstances: [{...processInstance}],
      totalCount: 1,
    });

    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);

    processStatisticsStore.fetchProcessStatistics();

    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual(
        mockProcessStatistics,
      ),
    );
  });

  it('should not fetch process statistics depending on completed operations if process and version filter does not exist', async () => {
    const processInstance = mockProcessInstances.processInstances[0]!;

    mockFetchProcessInstances().withSuccess({
      processInstances: [{...processInstance, hasActiveOperation: true}],
      totalCount: 1,
    });

    processStatisticsStore.init();
    processInstancesStore.init();
    processInstancesStore.fetchProcessInstancesFromFilters();

    await waitFor(() =>
      expect(processInstancesStore.state.status).toBe('fetched'),
    );
    expect(processStatisticsStore.state.statistics).toEqual([]);

    mockFetchProcessInstances().withSuccess({
      processInstances: [{...processInstance}],
      totalCount: 2,
    });

    processInstancesStore.fetchProcessInstancesFromFilters();

    await waitFor(() =>
      expect(processInstancesStore.state.filteredProcessInstancesCount).toBe(2),
    );

    expect(processStatisticsStore.state.statistics).toEqual([]);
  });

  it('should retry fetch on network reconnection', async () => {
    const eventListeners: any = {};
    const originalEventListener = window.addEventListener;
    window.addEventListener = jest.fn((event: string, cb: any) => {
      eventListeners[event] = cb;
    });

    mockFetchProcessInstancesStatistics().withNetworkError();

    processStatisticsStore.init();
    processStatisticsStore.fetchProcessStatistics();

    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual([]),
    );

    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);

    eventListeners.online();

    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual(
        mockProcessStatistics,
      ),
    );

    window.addEventListener = originalEventListener;
  });
});
