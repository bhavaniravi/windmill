import * as React from "react";
import styled from "styled-components";
import { FaSync, FaCaretDown, FaCaretRight, FaSearch } from "react-icons/fa";
import { AirflowOperator, IAirflowOperator } from ".";
import { SidebarTitle, BaseSidebar, Theme } from "../Theme";

const RefreshSplit = styled.div`
  min-width: 250px;
`;

const RefreshText = styled.div`
  float: left;
`;

const RefreshButton = styled.div`
  color: ${Theme.colors.brand};
  border-radius: 20px;
  float: right;
  transition: 0.3s ease all;
  cursor: pointer;
  &:hover {
    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.1);
  }
  &:active {
    background: #5682d2;
  }
`;

const distinct = (value, index, self) => {
  return self.indexOf(value) === index;
};

const prettyName = (name: string) => {
  return name
    .split(".")
    .pop()
    .split("_")
    .map((val: string) =>
      "".concat(val.slice(0, 1).toUpperCase(), val.slice(1))
    )
    .join(" ");
};

const DropdownTitle = styled.div`
  padding: 10px 10px;
  transition: 0.3s ease all;
  cursor: pointer;
  border: 1px solid ${Theme.colors.lightAccent};
  &:hover {
    box-shadow: 0 10px 20px rgba(0, 0, 0, 0.1);
  }
  &:active {
    background: ${Theme.colors.lightAccent};
  }
`;

const SearchDiv = styled.div`
  border: 1px solid ${Theme.colors.lightAccent};
  display: flex;
  flex-direction: row;
`;

const SearchBar = styled.input`
  margin: 10px 20px;
  padding: 7px;
  border: 1px solid ${Theme.colors.brand};
  border-radius: 3px;
  &:hover {
    background: ${Theme.colors.light};
  }
`;

const SearchIcon = styled.div`
  color: ${Theme.colors.brand};
  margin: 20px 0px;
`;

const ModuleDiv = styled.div`
  flex: 1;
  overflow-y: scroll;
`;

class AirflowModule extends React.Component<{
  moduleName: string;
  isModuleOpen: Function;
  setModule: Function;
}> {
  public constructor(props) {
    super(props);

    this.handleDropDown = this.handleDropDown.bind(this);
    this.handleFoldUp = this.handleFoldUp.bind(this);
  }

  public get isOpen(): boolean {
    return this.props.isModuleOpen(this);
  }

  handleDropDown() {
    this.props.setModule(this);
  }

  handleFoldUp() {
    this.props.setModule(null);
  }

  public render() {
    return React.Children.toArray(this.props.children).length === 1 ? (
      this.isOpen ? (
        <div>
          <DropdownTitle onClick={this.handleFoldUp}>
            <FaCaretDown />
            {this.props.moduleName}
          </DropdownTitle>
          {this.props.children}
        </div>
      ) : (
        <div>
          <DropdownTitle onClick={this.handleDropDown}>
            <FaCaretRight />
            {this.props.moduleName}
          </DropdownTitle>
        </div>
      )
    ) : (
      <div />
    );
  }
}

export class AirflowPanel extends React.Component<
  {
    operators: IAirflowOperator[];
    refreshOperators: Function;
  },
  {
    openComponent: AirflowModule;
    searchValue: string;
  }
> {
  public constructor(props) {
    super(props);

    this.handleSearch = this.handleSearch.bind(this);
    this.handleRefresh = this.handleRefresh.bind(this);
    this.isModuleOpen = this.isModuleOpen.bind(this);
    this.setOpenModule = this.setOpenModule.bind(this);
  }

  state = {
    openComponent: null,
    searchValue: " "
  };

  public handleRefresh() {
    this.props.refreshOperators();
  }

  public handleSearch(event: React.ChangeEvent<HTMLInputElement>) {
    const val = event.target.value;
    this.setState(prevState => ({
      ...prevState,
      searchValue: val
    }));
  }

  public isModuleOpen(moduleComponent: AirflowModule) {
    return moduleComponent === this.state.openComponent;
  }

  public setOpenModule(moduleComponent: AirflowModule) {
    this.setState({
      openComponent: moduleComponent
    });
  }

  public getModules() {
    return this.props.operators
      .map((operator: IAirflowOperator) => operator.properties.module)
      .filter(distinct)
      .map(prettyName);
  }

  public render() {
    return (
      <BaseSidebar>
        <SidebarTitle>
          <RefreshSplit>
            <RefreshText>Operator Library</RefreshText>
            <RefreshButton onClick={this.handleRefresh}>
              <FaSync />
            </RefreshButton>
          </RefreshSplit>
        </SidebarTitle>
        <SearchDiv>
          <SearchBar
            placeholder={"Search operators"}
            onChange={this.handleSearch}
          />
          <SearchIcon>
            <FaSearch />
          </SearchIcon>
        </SearchDiv>
        <ModuleDiv>
          {this.props.operators ? (
            [].concat(
              ...this.getModules().map((mod: string) => (
                <AirflowModule
                  moduleName={mod}
                  isModuleOpen={this.isModuleOpen}
                  setModule={this.setOpenModule}
                >
                  {[].concat(
                    ...this.props.operators
                      .filter(
                        (operator: IAirflowOperator) =>
                          prettyName(operator.properties.module) == mod
                      )
                      // .filter(
                      //   (operator: IAirflowOperator) =>
                      //     operator.properties.module.search(
                      //       this.state.searchValue
                      //     ) != -1
                      // )
                      .map((operator: IAirflowOperator, i: Number) => (
                        <AirflowOperator
                          {...operator}
                          key={`${operator.type}-${i}`}
                        />
                      ))
                  )}
                </AirflowModule>
              ))
            )
          ) : (
            <p>Loading operators...</p>
          )}
        </ModuleDiv>
      </BaseSidebar>
    );
  }
}
