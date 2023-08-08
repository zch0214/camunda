/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import {render, screen, waitFor, within} from 'modules/testing-library';
import {getWrapper} from './mocks';
import {processesStore} from 'modules/stores/processes';
import {processDiagramStore} from 'modules/stores/processDiagram';
import {
  groupedProcessesMock,
  mockProcessStatistics,
  mockProcessXML,
} from 'modules/testUtils';
import {Filters} from '../index';
import {mockFetchGroupedProcesses} from 'modules/mocks/api/processes/fetchGroupedProcesses';
import {mockFetchProcessInstancesStatistics} from 'modules/mocks/api/processInstances/fetchProcessInstancesStatistics';
import {mockFetchProcessXML} from 'modules/mocks/api/processes/fetchProcessXML';
import {pickDateTimeRange} from 'modules/testUtils/dateTimeRange';
import {
  selectFlowNode,
  selectProcess,
  selectProcessVersion,
} from 'modules/testUtils/selectComboBoxOption';

jest.unmock('modules/utils/date/formatDate');

describe('Filters', () => {
  beforeEach(async () => {
    mockFetchGroupedProcesses().withSuccess(groupedProcessesMock);
    mockFetchProcessInstancesStatistics().withSuccess(mockProcessStatistics);
    mockFetchProcessXML().withSuccess(mockProcessXML);

    processesStore.fetchProcesses();

    await processDiagramStore.fetchProcessDiagram('bigVarProcess');
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('should load the process and version fields', async () => {
    jest.useFakeTimers();
    const {user} = render(<Filters />, {
      wrapper: getWrapper(),
    });

    await waitFor(() => expect(screen.getByLabelText('Name')).toBeEnabled());
    await selectProcess({user, option: 'Big variable process'});

    expect(
      screen.getByLabelText('Version', {selector: 'button'}),
    ).toBeEnabled();
    await selectProcessVersion({user, option: '1'});

    await waitFor(() =>
      expect(screen.getByTestId('search')).toHaveTextContent(
        /^\?process=bigVarProcess&version=1$/,
      ),
    );

    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('should load values from the URL', async () => {
    const MOCK_PARAMS = {
      process: 'bigVarProcess',
      version: '1',
      ids: '2251799813685467',
      parentInstanceId: '1954699813693756',
      errorMessage: 'a random error',
      flowNodeId: 'ServiceTask_0kt6c5i',
      variableName: 'foo',
      variableValue: 'bar',
      operationId: '2f5b1beb-cbeb-41c8-a2f0-4c0bcf76c4ee',
      active: 'true',
      incidents: 'true',
      completed: 'true',
      canceled: 'true',
    } as const;

    render(<Filters />, {
      wrapper: getWrapper(
        `/?${new URLSearchParams(Object.entries(MOCK_PARAMS)).toString()}`,
      ),
    });

    await waitFor(() =>
      expect(
        screen.getByRole('combobox', {
          name: 'Name',
        }),
      ).toBeEnabled(),
    );
    expect(
      screen.getByRole('combobox', {
        name: 'Name',
      }),
    ).toHaveValue('Big variable process');
    expect(
      screen.getByLabelText('Version', {selector: 'button'}),
    ).toHaveTextContent('1');

    expect(screen.getByLabelText('Flow Node')).toHaveValue(
      MOCK_PARAMS.flowNodeId,
    );

    expect(screen.getByDisplayValue(MOCK_PARAMS.ids)).toBeInTheDocument();

    expect(
      screen.getByDisplayValue(MOCK_PARAMS.parentInstanceId),
    ).toBeInTheDocument();

    expect(
      screen.getByDisplayValue(MOCK_PARAMS.errorMessage),
    ).toBeInTheDocument();

    expect(
      screen.getByDisplayValue(MOCK_PARAMS.variableName),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.variableValue),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.operationId),
    ).toBeInTheDocument();
    expect(screen.getByTestId(/active/)).toBeChecked();
    expect(screen.getByTestId(/incidents/)).toBeChecked();
    expect(screen.getByTestId(/completed/)).toBeChecked();
    expect(screen.getByTestId(/canceled/)).toBeChecked();
  });

  it('should load values from the URL - date ranges', async () => {
    const MOCK_PARAMS = {
      startDateAfter: '2021-02-21 09:00:00',
      startDateBefore: '2021-02-22 10:00:00',
      endDateAfter: '2021-02-23 11:00:00',
      endDateBefore: '2021-02-24 12:00:00',
    } as const;

    const initialPath = `/?${new URLSearchParams(
      Object.entries(MOCK_PARAMS),
    ).toString()}`;

    render(<Filters />, {
      wrapper: getWrapper(initialPath),
    });

    // Wait for data to be fetched
    await waitFor(() => expect(screen.getByLabelText('Name')).toBeEnabled());

    // Hidden fields
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.endDateAfter),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.endDateBefore),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.endDateAfter),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue(MOCK_PARAMS.endDateBefore),
    ).toBeInTheDocument();
    // Non-hidden fields
    expect(
      screen.getByDisplayValue('2021-02-21 09:00:00 - 2021-02-22 10:00:00'),
    ).toBeInTheDocument();
    expect(
      screen.getByDisplayValue('2021-02-23 11:00:00 - 2021-02-24 12:00:00'),
    ).toBeInTheDocument();
  });

  it('should set modified values to the URL', async () => {
    const MOCK_VALUES = {
      process: 'bigVarProcess',
      version: '1',
      ids: '2251799813685462',
      parentInstanceId: '1954699813693756',
      errorMessage: 'an error',
      flowNodeId: 'ServiceTask_0kt6c5i',
      variableName: 'variableFoo',
      variableValue: 'true',
      operationId: '90fdfe82-090b-4d84-af31-5db612514191',
      active: 'true',
      incidents: 'true',
      completed: 'true',
      canceled: 'true',
    } as const;
    const {user} = render(<Filters />, {
      wrapper: getWrapper(),
    });

    // Wait for data to be fetched
    await waitFor(() => expect(screen.getByLabelText('Name')).toBeEnabled());
    await waitFor(() =>
      expect(screen.getByLabelText('Flow Node')).toBeEnabled(),
    );

    expect(screen.getByLabelText('Name')).toHaveValue('');
    expect(
      screen.getByLabelText('Version', {selector: 'button'}),
    ).toBeDisabled();
    expect(screen.getByLabelText('Flow Node')).toHaveValue('');
    expect(screen.getByTestId(/active/)).not.toBeChecked();
    expect(screen.getByTestId(/incidents/)).not.toBeChecked();
    expect(screen.getByTestId(/completed/)).not.toBeChecked();
    expect(screen.getByTestId(/canceled/)).not.toBeChecked();

    await selectProcess({user, option: 'Big variable process'});

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Process Instance Key(s)'));
    await user.type(
      screen.getByLabelText(/process instance key\(s\)/i),
      MOCK_VALUES.ids,
    );

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Parent Process Instance Key'));
    await user.type(
      screen.getByLabelText(/Parent Process Instance Key/i),
      MOCK_VALUES.parentInstanceId,
    );

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Error Message'));
    await user.type(
      screen.getByLabelText(/error message/i),
      MOCK_VALUES.errorMessage,
    );

    await selectFlowNode({user, option: MOCK_VALUES.flowNodeId});

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Variable'));
    await user.type(
      screen.getByTestId('optional-filter-variable-name'),
      MOCK_VALUES.variableName,
    );
    await user.type(screen.getByLabelText(/value/i), MOCK_VALUES.variableValue);

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Operation Id'));
    await user.type(
      screen.getByLabelText(/operation id/i),

      MOCK_VALUES.operationId,
    );

    await user.click(screen.getByTestId(/active/));
    await user.click(screen.getByTestId(/incidents/));
    await user.click(screen.getByTestId(/completed/));
    await user.click(screen.getByTestId(/canceled/));

    await waitFor(() =>
      expect(
        Object.fromEntries(
          new URLSearchParams(
            screen.getByTestId('search').textContent ?? '',
          ).entries(),
        ),
      ).toEqual(expect.objectContaining(MOCK_VALUES)),
    );
  });

  it('should set modified values to the URL - date ranges', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper(),
    });

    await waitFor(() => expect(screen.getByLabelText('Name')).toBeEnabled());

    await waitFor(() =>
      expect(screen.getByLabelText('Flow Node')).toBeEnabled(),
    );

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Start Date Range'));

    const startDate = await pickDateTimeRange({
      user,
      screen,
      fromDay: '5',
      toDay: '10',
    });
    await user.click(screen.getByText('Apply'));
    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('End Date Range'));

    const endDate = await pickDateTimeRange({
      user,
      screen,
      fromDay: '15',
      toDay: '20',
      fromTime: '11:22:33',
      toTime: '08:59:59',
    });
    await user.click(screen.getByText('Apply'));

    const MOCK_VALUES = {
      startDateAfter: startDate.fromDate,
      startDateBefore: startDate.toDate,
      endDateAfter: endDate.fromDate,
      endDateBefore: endDate.toDate,
    } as const;

    await waitFor(() => {
      return expect(
        Object.fromEntries(
          new URLSearchParams(
            screen.getByTestId('search').textContent ?? '',
          ).entries(),
        ),
      ).toEqual(expect.objectContaining(MOCK_VALUES));
    });
  });

  it('should have JSON editor for variable value filter', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper(),
    });

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Variable'));
    await user.click(screen.getByTitle(/open json editor modal/i));

    expect(
      within(screen.getByTestId('modal')).getByRole('button', {
        name: /cancel/i,
      }),
    ).toBeEnabled();
    expect(
      within(screen.getByTestId('modal')).getByRole('button', {name: /apply/i}),
    ).toBeEnabled();
    expect(
      within(screen.getByTestId('modal')).getByTestId('json-editor-container'),
    ).toBeInTheDocument();
  });

  it('should enable the reset button', async () => {
    jest.useFakeTimers();

    const {user} = render(<Filters />, {
      wrapper: getWrapper('/?active=true&incidents=true'),
    });

    expect(screen.getByTitle(/reset filters/i)).toBeDisabled();

    await user.click(screen.getByTestId(/incidents/));

    await waitFor(() =>
      expect(screen.getByTestId('search')).toHaveTextContent(/^\?active=true$/),
    );

    expect(screen.getByTitle(/reset filters/i)).toBeEnabled();

    jest.clearAllTimers();
    jest.useRealTimers();
  });

  it('should not submit an invalid form after deleting an optional filter', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper(),
    });
    expect(screen.getByTestId('search')).toBeEmptyDOMElement();

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Parent Process Instance Key'));
    await user.type(screen.getByLabelText(/parent process instance key/i), 'a');

    expect(
      await screen.findByText('Key has to be a 16 to 19 digit number'),
    ).toBeInTheDocument();

    expect(screen.getByTestId('search')).toBeEmptyDOMElement();
    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Operation Id'));
    await user.click(screen.getByTestId('delete-operationId'));

    expect(screen.getByTestId('search')).toBeEmptyDOMElement();
  });

  it('should be able to submit form after deleting an invalid optional filter', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper('/?active=true&incidents=true'),
    });
    expect(screen.getByTestId('search')).toHaveTextContent(
      /^\?active=true&incidents=true$/,
    );

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Parent Process Instance Key'));
    await user.type(screen.getByLabelText(/parent process instance key/i), 'a');

    expect(
      await screen.findByText('Key has to be a 16 to 19 digit number'),
    ).toBeInTheDocument();

    expect(screen.getByTestId('search')).toHaveTextContent(
      /^\?active=true&incidents=true$/,
    );

    await user.click(screen.getByTestId('delete-parentInstanceId'));

    await user.click(screen.getByText(/^more filters$/i));
    await user.click(screen.getByText('Error Message'));
    await user.type(screen.getByLabelText(/error message/i), 'test');

    await waitFor(() =>
      expect(screen.getByTestId('search')).toHaveTextContent(
        /^\?active=true&incidents=true&errorMessage=test$/,
      ),
    );
  });

  it('Should order optional filters', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper('/?active=true&incidents=true'),
    });

    const optionalFilters: Array<{name: string; fields: string[]}> = [
      {name: 'Error Message', fields: ['Error Message']},
      {
        name: 'Parent Process Instance Key',
        fields: ['Parent Process Instance Key'],
      },
      {name: 'Variable', fields: ['Name', 'Value']},
      {name: 'Process Instance Key(s)', fields: ['Process Instance Key(s)']},
      {name: 'Operation Id', fields: ['Operation Id']},
    ];

    let fieldLabels = optionalFilters.reduce((acc, optionalFilter) => {
      return [...acc, ...optionalFilter.fields];
    }, [] as string[]);

    for (let i = 0; i < optionalFilters.length; i++) {
      await user.click(screen.getByText('More Filters'));
      await user.click(screen.getByText(optionalFilters[i]!.name));
    }

    let visibleOptionalFilters = screen.getAllByTestId(/^optional-filter-/i);

    for (let i = 0; i < visibleOptionalFilters.length; i++) {
      expect(
        within(visibleOptionalFilters[i]?.parentElement!).getByText(
          fieldLabels[i]!,
        ),
      ).toBeInTheDocument();
    }

    await user.click(screen.getByText(/reset filters/i));

    for (let i = 0; i < visibleOptionalFilters.length; i++) {
      expect(
        within(visibleOptionalFilters[i]?.parentElement!).queryByText(
          fieldLabels[i]!,
        ),
      ).not.toBeInTheDocument();
    }

    optionalFilters.reverse();
    fieldLabels = optionalFilters.reduce((acc, optionalFilter) => {
      return [...acc, ...optionalFilter.fields];
    }, [] as string[]);

    for (let i = 0; i < optionalFilters.length; i++) {
      await user.click(screen.getByText('More Filters'));
      await user.click(screen.getByText(optionalFilters[i]!.name));
    }

    visibleOptionalFilters = screen.getAllByTestId(/^optional-filter-/i);

    for (let i = 0; i < visibleOptionalFilters.length; i++) {
      expect(
        within(visibleOptionalFilters[i]?.parentElement!).getByText(
          fieldLabels[i]!,
        ),
      ).toBeInTheDocument();
    }
  });

  // TODO: enable when https://github.com/camunda/operate/issues/4403 is implemented again
  it.skip('should omit all versions option', async () => {
    const {user} = render(<Filters />, {
      wrapper: getWrapper(
        `/?${new URLSearchParams(
          Object.entries({
            process: 'bigVarProcess',
            version: '1',
          }),
        ).toString()}`,
      ),
    });

    await user.click(screen.getByLabelText(/version/i));

    expect(
      within(screen.getByLabelText(/version/i)!).getByRole('option', {
        name: '1',
      }),
    ).toBeInTheDocument();

    expect(
      within(screen.queryByLabelText(/version/i)!).queryByRole('option', {
        name: /all/i,
      }),
    ).not.toBeInTheDocument();
  });
});
