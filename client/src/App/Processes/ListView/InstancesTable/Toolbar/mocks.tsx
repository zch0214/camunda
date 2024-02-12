/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import {useEffect} from 'react';
import {observer} from 'mobx-react';
import {MemoryRouter} from 'react-router-dom';
import {Screen, waitFor} from '@testing-library/react';
import {Paths} from 'modules/Routes';
import {ProcessInstancesDto} from 'modules/api/processInstances/fetchProcessInstances';
import {processInstanceMigrationStore} from 'modules/stores/processInstanceMigration';
import {processInstancesStore} from 'modules/stores/processInstances';
import {processInstancesSelectionStore} from 'modules/stores/processInstancesSelection';
import {processStatisticsStore} from 'modules/stores/processStatistics/processStatistics.migration.source';
import {UserEvent} from '@testing-library/user-event';
import {processXmlStore} from 'modules/stores/processXml/processXml.list';

const fetchProcessInstances = async (screen: Screen, user: UserEvent) => {
  await user.click(
    screen.getByRole('button', {name: /fetch process instances/i}),
  );
  await waitFor(() =>
    expect(processInstancesStore.state.status).toBe('fetched'),
  );
};

const fetchProcessXml = async (screen: Screen, user: UserEvent) => {
  await user.click(screen.getByRole('button', {name: /fetch process xml/i}));
  await waitFor(() =>
    expect(processXmlStore.state.diagramModel).not.toBeNull(),
  );
};

const getProcessInstance = (
  state: ProcessInstanceEntity['state'],
  mockData: ProcessInstancesDto,
) => {
  const instance = mockData.processInstances.find(
    (instance) => instance.state === state,
  );

  if (instance === undefined) {
    throw new Error(`please make sure there is a ${state} in mockData`);
  }

  return instance;
};

function getWrapper(initialPath: string = Paths.processes()) {
  const Wrapper: React.FC<{children?: React.ReactNode}> = observer(
    ({children}) => {
      useEffect(() => {
        return () => {
          processInstancesSelectionStore.reset();
          processInstancesStore.reset();
          processInstanceMigrationStore.reset();
          processStatisticsStore.reset();
          processXmlStore.reset();
        };
      }, []);
      return (
        <MemoryRouter initialEntries={[initialPath]}>
          {children}
          <button
            onClick={processInstancesSelectionStore.selectAllProcessInstances}
          >
            Select all instances
          </button>
          <button
            onClick={() =>
              processInstancesStore.fetchInstances({
                fetchType: 'initial',
                payload: {query: {}},
              })
            }
          >
            Fetch process instances
          </button>
          <button
            onClick={() => {
              processXmlStore.fetchProcessXml('1');
            }}
          >
            Fetch process xml
          </button>
        </MemoryRouter>
      );
    },
  );

  return Wrapper;
}

export {getWrapper, getProcessInstance, fetchProcessInstances, fetchProcessXml};