/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import {
  render,
  screen,
  waitFor,
  waitForElementToBeRemoved,
  within,
} from 'modules/testing-library';
import {Route, MemoryRouter, Routes, Link} from 'react-router-dom';
import {Processes} from '../index';
import {
  groupedProcessesMock,
  mockProcessStatistics,
  mockProcessXML,
  mockProcessInstances,
} from 'modules/testUtils';
import {processInstancesSelectionStore} from 'modules/stores/processInstancesSelection';
import {processInstancesStore} from 'modules/stores/processInstances';
import {processXmlStore} from 'modules/stores/processXml';
import {processesStore} from 'modules/stores/processes';
import {LocationLog} from 'modules/utils/LocationLog';
import {AppHeader} from 'App/Layout/AppHeader';
import {mockFetchProcessInstances} from 'modules/mocks/api/processInstances/fetchProcessInstances';
import {mockFetchGroupedProcesses} from 'modules/mocks/api/processes/fetchGroupedProcesses';
import {mockFetchProcessInstancesStatistics} from 'modules/mocks/api/processInstances/fetchProcessInstancesStatistics';
import {mockFetchProcessXML} from 'modules/mocks/api/processes/fetchProcessXML';
import {useEffect} from 'react';
import {Paths} from 'modules/Routes';
import {mockFetchBatchOperations} from 'modules/mocks/api/fetchBatchOperations';
import {notificationsStore} from 'modules/stores/notifications';
import {processStatisticsStore} from 'modules/stores/processStatistics';

jest.mock('modules/utils/bpmn');
const handleRefetchSpy = jest.spyOn(processesStore, 'handleRefetch');
jest.mock('modules/stores/notifications', () => ({
  notificationsStore: {
    displayNotification: jest.fn(() => () => {}),
  },
}));

function getWrapper(initialPath: string = Paths.processes()) {
  const Wrapper: React.FC<{children?: React.ReactNode}> = ({children}) => {
    useEffect(() => {
      return () => {
        processInstancesSelectionStore.reset();
        processInstancesStore.reset();
        processXmlStore.reset();
        processStatisticsStore.reset();
        processesStore.reset();
      };
    }, []);
    return (
      <MemoryRouter initialEntries={[initialPath]}>
        <Routes>
          <Route path={Paths.processes()} element={children} />
        </Routes>
        <Link to={`${Paths.processes()}?active=true`}>go to active</Link>
        <Link
          to={`${Paths.processes()}?process=eventBasedGatewayProcess&version=1`}
        >
          go to event based
        </Link>
        <Link to={Paths.processes()}>go to no filters</Link>
        <LocationLog />
      </MemoryRouter>
    );
  };

  return Wrapper;
}

describe('Instances', () => {
  beforeEach(() => {
    mockFetchProcessInstances().withSuccess(mockProcessInstances);
    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);
    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);
    mockFetchProcessXML().withSuccess(mockProcessXML);
    mockFetchBatchOperations().withSuccess([]);
  });

  it('should render title and document title', async () => {
    render(<Processes />, {
      wrapper: getWrapper(`${Paths.processes()}?incidents=true&active=true`),
    });

    expect(screen.getByText('Operate Process Instances')).toBeInTheDocument();
    expect(document.title).toBe('Operate: Process Instances');
    expect(
      await screen.findByText('There is no Process selected'),
    ).toBeInTheDocument();
    expect(
      await screen.findByRole('heading', {
        name: /process instances - 912 results/i,
      }),
    ).toBeInTheDocument();
  });

  it('should render page components', async () => {
    render(<Processes />, {
      wrapper: getWrapper(`${Paths.processes()}?active=true&incidents=true`),
    });

    // diagram panel
    expect(
      screen.getByRole('heading', {name: 'Process', level: 2}),
    ).toBeInTheDocument();

    // filters panel
    expect(
      screen.getByRole('heading', {name: 'Process', level: 3}),
    ).toBeInTheDocument();

    expect(
      await screen.findByText('There is no Process selected'),
    ).toBeInTheDocument();
    expect(
      screen.getByText(
        'To see a Diagram, select a Process in the Filters panel',
      ),
    ).toBeInTheDocument();

    // filters panel
    expect(screen.getByRole('heading', {name: /Filter/})).toBeInTheDocument();

    // instances table
    expect(
      await screen.findByRole('heading', {
        name: /process instances - 912 results/i,
      }),
    ).toBeInTheDocument();
  });

  it('should reset selected instances when filters change', async () => {
    mockFetchProcessInstances().withSuccess(mockProcessInstances);

    const {user} = render(<Processes />, {
      wrapper: getWrapper(`${Paths.processes()}?active=true&incidents=true`),
    });

    await waitForElementToBeRemoved(screen.getByTestId('data-table-skeleton'));

    const withinRow = within(
      screen.getByRole('row', {
        name: /2251799813685594/i,
      }),
    );

    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).not.toBeChecked();

    await user.click(withinRow.getByRole('checkbox', {name: /select row/i}));
    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).toBeChecked();

    mockFetchProcessInstances().withDelay(mockProcessInstances);
    await user.click(screen.getByText(/go to active/i));
    await waitForElementToBeRemoved(screen.getByTestId('data-table-loader'));

    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).not.toBeChecked();
  });

  it('should not reset selected instances when table is sorted', async () => {
    const {user} = render(<Processes />, {
      wrapper: getWrapper(`${Paths.processes()}?active=true&incidents=true`),
    });

    expect(
      await screen.findByRole('heading', {
        name: /process instances - 912 results/i,
      }),
    ).toBeInTheDocument();

    const withinRow = within(
      screen.getByRole('row', {
        name: /2251799813685594/i,
      }),
    );

    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).not.toBeChecked();

    await user.click(withinRow.getByRole('checkbox', {name: /select row/i}));
    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).toBeChecked();

    mockFetchProcessInstances().withDelay(mockProcessInstances);
    await user.click(screen.getByRole('button', {name: 'Sort by Name'}));

    expect(screen.getByTestId('data-table-loader')).toBeInTheDocument();
    await waitForElementToBeRemoved(screen.getByTestId('data-table-loader'));

    expect(
      withinRow.getByRole('checkbox', {name: /select row/i}),
    ).toBeChecked();
  });

  it('should fetch diagram and diagram statistics', async () => {
    const firstProcessStatisticsResponse = [
      {...mockProcessStatistics[0]!, completed: 10},
    ];

    mockFetchProcessInstances().withSuccess(mockProcessInstances);
    mockFetchProcessXML().withSuccess(mockProcessXML);
    mockFetchProcessInstancesStatistics().withSuccess(
      firstProcessStatisticsResponse,
    );

    const {user} = render(<Processes />, {
      wrapper: getWrapper(
        `${Paths.processes()}?process=bigVarProcess&version=1`,
      ),
    });

    await waitFor(() => expect(processXmlStore.state.status).toBe('fetched'));
    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual(
        firstProcessStatisticsResponse,
      ),
    );
    expect(processXmlStore.state.diagramModel).not.toBe(null);

    await user.click(screen.getByText(/go to event based/i));

    await waitFor(() => expect(processXmlStore.state.status).toBe('fetching'));

    await waitFor(() => expect(processXmlStore.state.status).toBe('fetched'));
    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual(
        mockProcessStatistics,
      ),
    );
    expect(processXmlStore.state.diagramModel).not.toBe(null);

    mockFetchProcessInstances().withSuccess({
      processInstances: [],
      totalCount: 0,
    });

    await user.click(screen.getByText(/go to no filters/i));

    await waitFor(() =>
      expect(processStatisticsStore.state.statistics).toEqual([]),
    );

    await waitFor(() =>
      expect(screen.queryByTestId('diagram-spinner')).not.toBeInTheDocument(),
    );
    await waitFor(() =>
      expect(screen.queryByTestId('data-table-loader')).not.toBeInTheDocument(),
    );
  });

  it('should refetch data when navigated from header', async () => {
    const {user} = render(
      <>
        <AppHeader />
        <Processes />
      </>,
      {
        wrapper: getWrapper(`${Paths.processes()}?active=true&incidents=true`),
      },
    );

    await waitForElementToBeRemoved(screen.getByTestId('data-table-skeleton'));

    await waitFor(() =>
      expect(screen.queryByTestId('diagram-spinner')).not.toBeInTheDocument(),
    );

    mockFetchProcessInstances().withDelay(mockProcessInstances);
    mockFetchGroupedProcesses().withDelay(groupedProcessesMock);

    await user.click(
      within(
        screen.getByRole('navigation', {
          name: /camunda operate/i,
        }),
      ).getByRole('link', {
        name: /processes/i,
      }),
    );
    expect(await screen.findByTestId('diagram-spinner')).toBeInTheDocument();
    expect(await screen.findByTestId('data-table-loader')).toBeInTheDocument();

    await waitFor(() =>
      expect(screen.queryByTestId('diagram-spinner')).not.toBeInTheDocument(),
    );

    await waitFor(() =>
      expect(screen.queryByTestId('data-table-loader')).not.toBeInTheDocument(),
    );
  });

  it('should poll 3 times for grouped processes and redirect to initial processes page if process does not exist', async () => {
    jest.useFakeTimers();

    const queryString =
      '?active=true&incidents=true&process=non-existing-process&version=all';

    const originalWindow = {...window};

    const locationSpy = jest.spyOn(window, 'location', 'get');

    locationSpy.mockImplementation(() => ({
      ...originalWindow.location,
      search: queryString,
    }));

    render(<Processes />, {
      wrapper: getWrapper(`${Paths.processes()}${queryString}`),
    });

    expect(screen.getByTestId('data-table-skeleton')).toBeInTheDocument();

    expect(screen.getByTestId('search').textContent).toBe(queryString);

    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);

    await waitFor(() => expect(processesStore.state.status).toBe('fetching'));
    expect(handleRefetchSpy).toHaveBeenCalledTimes(1);

    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);

    jest.runOnlyPendingTimers();

    await waitFor(() => expect(handleRefetchSpy).toHaveBeenCalledTimes(2));

    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);

    jest.runOnlyPendingTimers();
    await waitFor(() => expect(handleRefetchSpy).toHaveBeenCalledTimes(3));

    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);

    mockFetchProcessInstances().withSuccess({
      processInstances: [],
      totalCount: 0,
    });

    expect(screen.getByTestId('diagram-spinner')).toBeInTheDocument();
    expect(screen.getByTestId('data-table-skeleton')).toBeInTheDocument();

    jest.runOnlyPendingTimers();

    await waitFor(() => {
      expect(processesStore.processes.length).toBe(5);
      expect(screen.getByTestId('pathname')).toHaveTextContent(/^\/processes/);
      expect(screen.getByTestId('search').textContent).toBe(
        '?active=true&incidents=true',
      );
    });

    expect(notificationsStore.displayNotification).toHaveBeenCalledWith({
      isDismissable: true,
      kind: 'error',
      title: 'Process could not be found',
    });

    jest.clearAllTimers();
    jest.useRealTimers();

    locationSpy.mockRestore();
  });
});
