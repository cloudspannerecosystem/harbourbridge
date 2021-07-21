import "./../../components/Tab/Tab.component.js";
import "./../../components/TableCarousel/TableCarousel.component.js";
import "./../../components/StatusLegend/StatusLegend.component.js";
import "./../../components/Search/Search.component.js";
import "../../components/SiteButton/SiteButton.component.js";
import "../../components/EditGlobalDataTypeForm/EditGlobalDataTypeForm.componenet.js";
import "./../../components/Modal/Modal.component.js";
import { initSchemaScreenTasks } from "./../../helpers/SchemaConversionHelper.js";

// Services
import Actions from "./../../services/Action.service.js";
import Store from "./../../services/Store.service.js";
import "../../services/Fetch.service.js";

// constants

class SchemaConversionScreen extends HTMLElement {

    get testing() {
        return this.getAttribute("testing");
    }

    connectedCallback() {
        this.stateObserver = setInterval(this.observeState, 150);
        Actions.showSpinner()
        this.render();
    }

    disconnectedCallback() {
        clearInterval(this.stateObserver);
    }

    sendDatatoReportTab(tableNameArray, currentTabContent) {
        for (let i = 0; i < tableNameArray.length; i++) {
            let filterdata = {
                SpSchema: currentTabContent.SpSchema[tableNameArray[i]],
                SrcSchema: currentTabContent.SrcSchema[tableNameArray[i]],
                ToSource: currentTabContent.ToSource[tableNameArray[i]],
                ToSpanner: currentTabContent.ToSpanner[tableNameArray[i]],
                summary: Store.getinstance().tableData["summaryTabContent"][tableNameArray[i]],
                currentPageNumber: Actions.getCurrentPageNumber(i)
            };
            let component = document.querySelector(`#reportTab${i}`);
            component.data = filterdata;
        }
    }

    set Data(data) {
        this.data = data;
        this.render();
    }

    getChangingValue(currentTab) {
        currentTab = currentTab.substring(0, currentTab.length - 3);
        let currentArray = Actions.carouselStatus(currentTab);
        let flag = "Expand All";
        for (let i = 0; i < currentArray.length; i++) {
            if (currentArray[i] == true) {
                flag = "Collapse All";
            }
        }
        return flag;
    }

    observeState = () => {
        let updatedData = Store.getinstance();
        if (JSON.stringify(updatedData) !== JSON.stringify(this.data)) {
            this.data = JSON.parse(JSON.stringify(updatedData));
            this.render();
        }
    };

    render() {
        if (!this.data) {
            return;
        }
        const { currentTab, tableData, tableBorderData, searchInputValue } = this.data;
        let currentTabContent = tableData[`${currentTab}Content`];
        if (Object.keys(currentTabContent).length == 0) {
            Actions.hideSpinner();
            return;
        }
        const changingText = this.getChangingValue(currentTab);
        let tableNameArray;
        if (currentTab === "reportTab") {
            tableNameArray = Object.keys(currentTabContent.SpSchema)
                .filter((title) => title.indexOf(searchInputValue[currentTab]) > -1);
        } else {
            tableNameArray = Object.keys(currentTabContent)
                .filter((title) => title.indexOf(searchInputValue[currentTab]) > -1);
        }

        this.innerHTML = `
    <div class="summary-main-content" id='schema-screen-content'>
      <div id="snackbar"></div>
      <div>
        <h4 class="report-header">Schema Conversion Report
            <hb-site-button buttonid="download-schema" classname="download-button" 
                buttonaction="downloadSession" text="Download Session File"></hb-site-button>
        </h4>
      </div>
      <div class="report-tabs">
           <hb-tab currentTab = ${currentTab}>
      </div>
      <div class="status-icons">
        <hb-search tabid="${currentTab}" class="inlineblock" ></hb-search>
        <hb-status-legend></hb-status-legend> 
      </div> 
      <div class="tab-bg" id='tabBg'>
        <div class="tab-content">
          ${currentTab === 'reportTab' ? `<div id="report" class="tab-pane fade show active">
            <div class="accordion md-accordion" id="accordion" role="tablist" aria-multiselectable="true">
            ${tableNameArray.length > 0 ? `<hb-site-button buttonid="reportExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>`:''}
            ${tableNameArray.length > 0 ? `<hb-site-button buttonid="editButton" classname="expand right-align" buttonaction="editGlobalDataType" text="Edit Global Data Type"></hb-site-button>` :''}
              <div id='reportDiv'>
                ${tableNameArray.map((tableName, index) => {
                    return `
                    <hb-table-carousel tableTitle="${tableName}" id="${currentTab}${index}" tabId="report" 
                    tableIndex="${index}" borderData = "${tableBorderData[tableName]}"></hb-table-carousel>`;
                  })
                  .join("")}                    
                </div>
            </div>
            ${tableNameArray.length <=0 ? '<h5 class="no-text" >No Match Found</h5>':''}
          </div>`
              : `<div></div>`
          }

          ${
            currentTab === "ddlTab"
              ? `<div id="ddl" class="tab-pane fade show active">
            <div class="panel-group" id="ddl-accordion">
            ${tableNameArray.length > 0 ? `<hb-site-button buttonid="ddlExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>`:''}
            ${tableNameArray.length > 0 ?`<hb-site-button buttonid="download-ddl" classname="expand right-align" buttonaction="downloadDdl" text="Download DDL Statements"></hb-site-button>`:''}
              <div id='ddlDiv'>
                ${tableNameArray.map((tableName, index) => {
                    return `
                    <hb-table-carousel tableTitle="${tableName}" stringData="${currentTabContent[tableName]}" tabId="ddl" id="${currentTab}${index}" tableIndex=${index} borderData = "${tableBorderData[tableName]}">
                    </hb-table-carousel>`;
                  })
                  .join("")} 
                </div>
            </div>
           ${tableNameArray.length <=0 ? '<h5 class="no-text" >No Match Found</h5>':''}
          </div>`
              : `<div></div>`
          }

          ${
            currentTab === "summaryTab"
              ? `<div id="summary" class="tab-pane fade show active">
            <div class="panel-group" id="summary-accordion">
            ${tableNameArray.length > 0 ?`<hb-site-button buttonid="summaryExpandButton" classname="expand" buttonaction="expandAll" text="${changingText}"></hb-site-button>`:''}
            ${tableNameArray.length > 0 ?`<hb-site-button buttonid="download-report" classname="expand right-align" buttonaction="downloadReport" text="Download Summary Report"></hb-site-button>`:''}
              <div id='summaryDiv'>
                ${tableNameArray.map((tableName, index) => {
                    return `
                    <hb-table-carousel  tableTitle="${tableName}" stringData="${currentTabContent[tableName]}" id="${currentTab}${index}" tabId="summary" 
                    tableIndex=${index} borderData = "${tableBorderData[tableName]}"></hb-table-carousel>`;
                  })
                  .join("")} 
              </div>
            </div>
            ${tableNameArray.length <=0 ? '<h5 class="no-text">No Match Found</h5>':''}
          </div>`
              : `<div></div>`
          }

          </div>
      </div>
    </div>
    <hb-modal modalId="globalDataTypeModal" content="<hb-edit-global-datatype-form></hb-edit-global-datatype-form>" 
      contentIcon="" connectIconClass="" modalBodyClass="edit-global-data-type" title="Global Data Type Mapping"></hb-modal>
    <hb-modal modalId="index-and-key-delete-warning" content="" contentIcon="warning" 
      connectIconClass="warning-icon" modalBodyClass="connection-modal-body" title="Warning"></hb-modal>
    <hb-modal modalId="editTableWarningModal" content="edit table" contentIcon="cancel" 
      connectIconClass="connect-icon-failure" modalBodyClass="connection-modal-body" title="Error Message"></hb-modal>
    <hb-modal modalId="createIndexModal" content="" contentIcon="" 
      connectIconClass="" modalBodyClass="" title="Select keys for new index"></hb-modal>`;

    initSchemaScreenTasks();
    if (currentTab === "reportTab" && !this.testing) {
      this.sendDatatoReportTab(tableNameArray
        .filter((title)=>title.indexOf(searchInputValue[currentTab]) > -1), currentTabContent);
       if(!this.testing){ 
          window.scrollTo(0,Actions.getPageYOffset());
       }
    }
    Actions.hideSpinner();
  }
  constructor() {
    super();
  }
}

window.customElements.define("hb-schema-conversion-screen", SchemaConversionScreen);