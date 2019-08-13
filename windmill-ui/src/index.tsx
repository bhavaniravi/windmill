import { FlowChart, IChart } from "@mrblenny/react-flow-chart";
import * as actions from "@mrblenny/react-flow-chart/src/container/actions";
import { cloneDeep, mapValues } from "lodash";
import * as React from "react";
import { render } from "react-dom";
import styled from "styled-components";
import {
  DragAndDropSidebar,
  Page,
  SelectedSidebar,
  DropdownNavbar
} from "./components";
import { chartSimple } from "./misc/exampleChartState";

const AppLayout = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
  max-width: 100vw;
  max-height: 100vh;
`;

const Content = styled.div`
  display: flex;
  flex-direction: column;
  flex: 1;
  overflow: hidden;
`;

const navigation = {
  brand: { name: "Windmill", to: "/" },
  links: [
    { name: "File", to: "/" },
    { name: "View", to: "/" },
    { name: "About", to: "/" }
  ]
};

class App extends React.Component {
  public state = cloneDeep(chartSimple);

  public renderSelectedSidebar(chart: IChart, onDeleteKey: Function) {
    return <SelectedSidebar chart={chart} onDeleteKey={onDeleteKey} />;
  }

  public render() {
    const { brand, links } = navigation;

    const chart = this.state;
    const stateActions = mapValues(actions, (func: any) => (...args: any) =>
      this.setState(func(...args))
    ) as typeof actions;

    return (
      <AppLayout>
        <Page>
          <DropdownNavbar brand={brand} links={links} />
        </Page>
        <Page>
          <DragAndDropSidebar />
          <Content>
            <FlowChart chart={chart} callbacks={stateActions} />
          </Content>
          {this.renderSelectedSidebar(chart, stateActions.onDeleteKey)}
        </Page>
      </AppLayout>
    );
  }
}

render(<App />, document.getElementById("root"));
