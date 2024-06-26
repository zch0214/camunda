/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */

import {makeObservable, observable, action, computed} from 'mobx';
import {getUser, UserDto} from 'modules/api/getUser';
import {login, Credentials} from 'modules/api/login';
import {logout} from 'modules/api/logout';
import {NetworkError} from 'modules/networkError';
import {getStateLocally, storeStateLocally} from 'modules/utils/localStorage';

type Permissions = Array<'read' | 'write'>;

type State = {
  status:
    | 'initial'
    | 'logged-in'
    | 'fetching-user-information'
    | 'user-information-fetched'
    | 'logged-out'
    | 'session-expired'
    | 'invalid-initial-session'
    | 'invalid-third-party-session';
  permissions: Permissions;
  displayName: string | null;
  canLogout: boolean;
  userId: string | null;
  salesPlanType: UserDto['salesPlanType'];
  roles: ReadonlyArray<string> | null;
  c8Links: UserDto['c8Links'];
  tenants: UserDto['tenants'];
};

const DEFAULT_STATE: State = {
  status: 'initial',
  permissions: ['read', 'write'],
  displayName: null,
  canLogout: false,
  userId: null,
  salesPlanType: null,
  roles: [],
  c8Links: {},
  tenants: null,
};

class Authentication {
  state: State = {...DEFAULT_STATE};
  constructor() {
    makeObservable(this, {
      state: observable,
      disableSession: action,
      expireSession: action,
      startLoadingUser: action,
      setUser: action,
      reset: action,
      resetUser: action,
      setStatus: action,
      endLogin: action,
      tenantsById: computed,
    });
  }

  disableSession = () => {
    this.resetUser();

    if (
      !window.clientConfig?.canLogout ||
      window.clientConfig?.isLoginDelegated
    ) {
      this.#handleThirdPartySessionExpiration();

      return;
    }

    this.setStatus('logged-out');
  };

  expireSession = () => {
    this.resetUser();

    if (
      !window.clientConfig?.canLogout ||
      window.clientConfig?.isLoginDelegated
    ) {
      this.#handleThirdPartySessionExpiration();

      return;
    }

    if (this.state.status === 'user-information-fetched') {
      this.setStatus('session-expired');

      return;
    }

    this.setStatus('invalid-initial-session');
  };

  #handleThirdPartySessionExpiration = () => {
    const wasReloaded = getStateLocally()?.wasReloaded;

    this.setStatus('invalid-third-party-session');

    if (wasReloaded) {
      return;
    }

    storeStateLocally({
      wasReloaded: true,
    });

    window.location.reload();
  };

  handleLogin = async (credentials: Credentials): Promise<Error | void> => {
    const response = await login(credentials);

    if (!response.isSuccess) {
      return new NetworkError(
        'Could not login credentials',
        response.statusCode,
      );
    }

    this.endLogin();

    return;
  };

  endLogin = () => {
    this.state.status = 'logged-in';
  };

  authenticate = async (): Promise<void | Error> => {
    this.startLoadingUser();

    const response = await getUser({
      onFailure: () => {
        this.expireSession();
      },
      onException: () => {
        this.disableSession();
      },
    });

    if (!response.isSuccess) {
      return new Error('Could not fetch user information');
    }

    this.setUser(response.data);
  };

  startLoadingUser = () => {
    this.state.status = 'fetching-user-information';
  };

  setUser = ({
    displayName,
    permissions,
    canLogout,
    userId,
    salesPlanType,
    roles,
    c8Links,
    tenants,
  }: UserDto) => {
    storeStateLocally({
      wasReloaded: false,
    });

    this.state.status = 'user-information-fetched';
    this.state.displayName = displayName;
    this.state.canLogout = canLogout;
    this.state.userId = userId;
    this.state.salesPlanType = salesPlanType;
    this.state.roles = roles ?? [];
    this.state.permissions = permissions ?? DEFAULT_STATE.permissions;
    this.state.c8Links = c8Links;
    this.state.tenants = tenants;
  };

  handleLogout = async () => {
    const response = await logout();

    if (!response.isSuccess) {
      return new Error('Could not logout');
    }

    this.disableSession();
  };

  hasPermission = (scopes: Permissions) => {
    return this.state.permissions.some((permission) =>
      scopes.includes(permission),
    );
  };

  handleThirdPartySessionSuccess = () => {
    if (this.state.status === 'invalid-third-party-session') {
      this.authenticate();
    }
  };

  setStatus = (status: State['status']) => {
    this.state.status = status;
  };

  get tenantsById() {
    return this.state.tenants?.reduce<{[key: string]: string}>(
      (tenantsById, {tenantId, name}) => ({
        ...tenantsById,
        [tenantId]: name,
      }),
      {},
    );
  }
  resetUser = () => {
    this.state.displayName = DEFAULT_STATE.displayName;
    this.state.canLogout = DEFAULT_STATE.canLogout;
    this.state.permissions = DEFAULT_STATE.permissions;
  };

  reset = () => {
    this.state = {...DEFAULT_STATE};
  };
}

export const authenticationStore = new Authentication();
export type {Permissions};
