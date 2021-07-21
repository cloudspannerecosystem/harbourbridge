import Store from "./Store.service.js";
import Fetch from "./Fetch.service.js";
import { readTextFile, showSnackbar } from "./../helpers/SchemaConversionHelper.js";

var keysList = [];
var orderId = 0;
var temp = {};
var tableData = {
    data: {}
};

/**
 * All the manipulations to the store happen via the actions mentioned in this module
 *
 */
const resetIndexModal = () => {
    keysList = [];
    orderId = 0;
    temp = {};
}

const Actions = (() => {
    return {

        trial: () => {
            return "1";
        },

        add: (a, b) => {
            return a + b;
        },

        resetStore: () => {
            Store.resetStore();
        },

        setPageYOffset: (value) => {
            if (Store.getTableChanges() == "expand") {
                let currTab = Store.getCurrentTab();
                let buttonId = currTab.substr(0, currTab.length - 3) + "ExpandButton"
                Actions.expandAll("Expand All", buttonId, 10, 20);
            }
            Store.setPageYOffset(value)
        },

        getPageYOffset: () => {
            return Store.getPageYOffset()
        },

        resetReportTableData: () => {
            tableData.data = {};
        },

        onLoadDatabase: async(dbType, dumpFilePath) => {
            let reportData, sourceTableFlag, reportDataResp, reportDataCopy, jsonReportDataResp, requestCode;
            reportData = await Fetch.getAppData("POST", "/convert/dump", { Driver: dbType, Path: dumpFilePath });
            reportDataCopy = reportData.clone();
            requestCode = reportData.status;
            reportDataResp = await reportData.text();
            if (requestCode != 200) {
                showSnackbar(reportDataResp, " redBg");
                Actions.hideSpinner();
                return false;
            } else {
                jsonReportDataResp = await reportDataCopy.json();
                if (Object.keys(jsonReportDataResp.SpSchema).length == 0) {
                    showSnackbar("Please select valid file", " redBg");
                    Actions.hideSpinner();
                    return false;
                } else {
                    jQuery("#loadDatabaseDumpModal").modal("hide");
                    Store.updatePrimaryKeys(jsonReportDataResp);
                    Store.updateTableData("reportTabContent", jsonReportDataResp);
                }
            }
            Store.setarraySize(Object.keys(jsonReportDataResp.SpSchema).length);
            sourceTableFlag = Store.getSourceDbName()
            return true;
        },

        onconnect: async(dbType, dbHost, dbPort, dbUser, dbName, dbPassword) => {
            let sourceTableFlag = "",
                response;
            let payload = { Driver: dbType, Database: dbName, Password: dbPassword, User: dbUser, Port: dbPort, Host: dbHost };
            response = await Fetch.getAppData("POST", "/connect", payload);
            if (response.ok) {
                if (dbType === "mysql") sourceTableFlag = "MySQL";
                else if (dbType === "postgres") sourceTableFlag = "Postgres";
                Store.setSourceDbName(sourceTableFlag)
                jQuery("#connectToDbModal").modal("hide");
                jQuery("#connectModalSuccess").modal();
            } else {
                jQuery("#connectToDbModal").modal("hide");
                jQuery("#connectModalFailure").modal();
            }
            return response;
        },

        showSchemaAssessment: async() => {
            let reportDataResp, reportData, sourceTableFlag;
            reportData = await Fetch.getAppData("GET", "/convert/infoschema");
            reportDataResp = await reportData.json();
            Store.updatePrimaryKeys(reportDataResp);
            Store.updateTableData("reportTabContent", reportDataResp);
            Store.setarraySize(Object.keys(reportDataResp.SpSchema).length);
            jQuery("#connectModalSuccess").modal("hide");
        },

        onLoadSessionFile: async(filePath) => {
            let driver = '',
                response, payload;
            let srcDb = Store.getSourceDbName()
            if (srcDb === 'MySQL') {
                driver = 'mysqldump';
            } else if (srcDb === 'Postgres') {
                driver = 'pg_dump';
            }
            payload = { "Driver": driver, "DBName": '', "FilePath": filePath };
            response = await Fetch.getAppData('POST', '/session/resume', payload);
            if (response.ok) {
                let responseCopy, textResponse, jsonResponse;
                responseCopy = response.clone();
                jsonResponse = await responseCopy.json();
                if (Object.keys(jsonResponse.SpSchema).length == 0) {
                    showSnackbar('Please select valid session file', ' redBg');
                    jQuery('#importButton').attr('disabled', 'disabled');
                    return false;
                } else {
                    Store.updatePrimaryKeys(jsonResponse);
                    Store.updateTableData("reportTabContent", jsonResponse);
                    Store.setarraySize(Object.keys(jsonResponse.SpSchema).length);
                    jQuery('#loadSchemaModal').modal('hide');
                    return true;
                }
            } else {
                showSnackbar('Please select valid session file', ' redBg');
                jQuery('#importButton').attr('disabled', 'disabled');
                return false;
            }
        },

        ddlSummaryAndConversionApiCall: async() => {
            let conversionRate, conversionRateJson, ddlData, ddlDataJson, summaryData, summaryDataJson;
            ddlData = await Fetch.getAppData("GET", "/ddl");
            summaryData = await Fetch.getAppData("GET", "/summary");
            conversionRate = await Fetch.getAppData("GET", "/conversion");
            if (ddlData.ok && summaryData.ok && conversionRate.ok) {
                ddlDataJson = await ddlData.json();
                summaryDataJson = await summaryData.json();
                conversionRateJson = await conversionRate.json();
                Store.updateTableData("ddlTabContent", ddlDataJson);
                Store.updateTableData("summaryTabContent", summaryDataJson);
                Store.updateTableBorderData(conversionRateJson);
            } else {
                return false;
            }
            return true;
        },

        sessionRetrieval: async(dbType) => {
            let sessionStorageArr, sessionInfo, sessionResp;
            sessionResp = await Fetch.getAppData("GET", "/session");
            sessionInfo = await sessionResp.json();
            sessionStorageArr = JSON.parse(sessionStorage.getItem("sessionStorage"));
            if (sessionStorageArr == undefined) sessionStorageArr = [];
            sessionInfo.sourceDbType = dbType;
            sessionStorageArr.unshift(sessionInfo);
            sessionStorage.setItem("sessionStorage", JSON.stringify(sessionStorageArr));
            sessionStorage.setItem('currentSessionIdx', 0)
        },

        resumeSessionHandler: async(index, sessionArray) => {
            Actions.showSpinner()
            let driver, path, dbName, sourceDb, pathArray, fileName, filePath;
            Store.setSourceDbName(sessionArray[index].sourceDbType)
            driver = sessionArray[index].driver;
            path = sessionArray[index].filePath;
            dbName = sessionArray[index].dbName;
            sourceDb = sessionArray[index].sourceDbType;
            pathArray = path.split("/");
            fileName = pathArray[pathArray.length - 1];
            filePath = "./" + fileName;
            readTextFile(filePath, async(error, text) => {
                if (error) {
                    let storage = JSON.parse(sessionStorage.getItem('sessionStorage'))
                    storage.splice(index, 1);
                    sessionStorage.setItem('sessionStorage', JSON.stringify(storage))
                    Actions.hideSpinner()
                    window.location.href = '/';
                    showSnackbar(error, " redBg");
                } else {
                    let payload = { Driver: driver, DBName: dbName, FilePath: path };
                    let res = JSON.parse(text);
                    sessionStorage.setItem('currentSessionIdx', index)
                    Store.updatePrimaryKeys(res);
                    Store.updateTableData("reportTabContent", res);
                    Store.setarraySize(Object.keys(res.SpSchema).length);
                    await Fetch.getAppData("POST", "/session/resume", payload);
                }
            });
            // Actions.hideSpinner();
        },

        SearchTable: (value, tabId) => {
            Store.setSearchInputValue(tabId, value)
        },

        expandAll: (text, buttonId, x = 0, y = 10) => {
            Actions.showSpinner()
            if (text === "Expand All") {
                document.getElementById(buttonId).innerHTML = "Collapse All";
                Store.expandAll(x, y);
            } else {
                Actions.showSpinner();
                document.getElementById(buttonId).innerHTML = "Expand All";
                Store.collapseAll(false);
            }
        },

        downloadSession: async() => {
            let reportJsonObj = JSON.stringify(Store.getinstance().tableData.reportTabContent);
            reportJsonObj = reportJsonObj.replaceAll("9223372036854776000", "9223372036854775807");
            jQuery("<a />", {
                    download: "session.json",
                    href: "data:application/json;charset=utf-8," + encodeURIComponent(reportJsonObj, null, 4),
                }).appendTo("body").click(function() { jQuery(this).remove(); })[0]
                .click();
        },

        downloadDdl: async() => {
            let ddlreport = await Fetch.getAppData("GET", "/schema");
            if (ddlreport.ok) {
                await ddlreport.text().then(function(result) {
                    localStorage.setItem("schemaFilePath", result);
                });
                let schemaFilePath = localStorage.getItem("schemaFilePath");
                if (schemaFilePath) {
                    let schemaFileName = schemaFilePath.split("/")[schemaFilePath.split("/").length - 1];
                    let filePath = "./" + schemaFileName;
                    readTextFile(filePath, function(error, text) {
                        jQuery("<a />", {
                                download: schemaFileName,
                                href: "data:application/json;charset=utf-8," + encodeURIComponent(text),
                            }).appendTo("body").click(function() { jQuery(this).remove(); })[0]
                            .click();
                    });
                }
                showSnackbar('try again ', 'red')
            }
        },

        downloadReport: async() => {
            let summaryreport = await Fetch.getAppData("GET", "/report");
            if (summaryreport.ok) {
                await summaryreport.text().then(function(result) {
                    localStorage.setItem("reportFilePath", result);
                });
                let reportFilePath = localStorage.getItem("reportFilePath");
                let reportFileName = reportFilePath.split("/")[reportFilePath.split("/").length - 1];
                let filePath = "./" + reportFileName;
                readTextFile(filePath, function(error, text) {
                    jQuery("<a />", {
                            download: reportFileName,
                            href: "data:application/json;charset=utf-8," + encodeURIComponent(text),
                        }).appendTo("body").click(function() { jQuery(this).remove(); })[0]
                        .click();
                });
            }
        },

        editGlobalDataType: () => {
            jQuery("#globalDataTypeModal").modal();
        },

        checkInterleaveConversion: async(tableName) => {
            let interleaveApiCall;
            interleaveApiCall = await Fetch.getAppData("GET", "/setparent?table=" + tableName);
            let interleaveApiCallResp = await interleaveApiCall.json();
            let value = interleaveApiCallResp.tableInterleaveStatus.Possible;
            Store.setInterleave(tableName, value);
        },

        setGlobalDataType: async() => {
            Actions.showSpinner()
            let globalDataTypeList = Store.getGlobalDataTypeList();
            let dataTypeListLength = Object.keys(globalDataTypeList).length;
            let dataTypeJson = {};
            for (var i = 0; i <= dataTypeListLength; i++) {
                var row = document.getElementById("data-type-row" + i);
                if (row) {
                    var cells = row.getElementsByTagName("td");
                    if (document.getElementById("data-type-option" + i) != null) {
                        for (var j = 0; j < cells.length; j++) {
                            if (j === 0) {
                                var key = cells[j].innerText;
                            } else {
                                dataTypeJson[key] = document.getElementById(
                                    "data-type-option" + i
                                ).value;
                            }
                        }
                    }
                }
            }
            let res = await Fetch.getAppData("POST", "/typemap/global", dataTypeJson);
            if (res) {
                res = await res.json();
                Store.updatePrimaryKeys(res);
                Store.updateTableData("reportTabContent", res);
                // Actions.hideSpinner()
            } else {
                Actions.hideSpinner()
            }
        },

        setGlobalDataTypeList: async() => {
            let res = await Fetch.getAppData("GET", "/typemap");
            if (res) {
                let result = await res.json();
                Store.setGlobalDataTypeList(result)
            } else {
                showSnackbar('Not able to fetch global datatype list !')
            }
        },

        dataTypeUpdate: (id, globalDataTypeList) => {
            let selectedValue = document.getElementById(id).value;
            let idNum = parseInt(id.match(/\d+/), 10);
            let dataTypeOptionArray = globalDataTypeList[document.getElementById("data-type-key" + idNum).innerHTML];
            for (let i = 0; i < dataTypeOptionArray.length; i++) {
                if (dataTypeOptionArray[i].T === selectedValue) {
                    if (dataTypeOptionArray[i].Brief !== "") {
                        document.getElementById(`warning${idNum}`).classList.add("show");
                        document.getElementById(`warning${idNum}`).classList.remove("hidden");
                    } else {
                        document.getElementById(`warning${idNum}`).classList.add("hidden");
                        document.getElementById(`warning${idNum}`).classList.remove("show");
                    }
                }
            }
        },

        fetchIndexFormValues: async(tableIndex, tableName, name, uniqueness) => {
            Actions.showSpinner()
            if (keysList.length === 0) {
                Actions.hideSpinner()
                showSnackbar("Please select atleast one key column to create a new index", " redBg");
                return;
            }
            let newIndex = {};
            let newIndexPos = 1;
            let jsonObj = Store.getinstance().tableData.reportTabContent;
            let table = jsonObj.SpSchema[tableName];
            newIndex["Name"] = name;
            newIndex["Table"] = table.Name;
            if (uniqueness) {
                newIndex["Unique"] = true;
            } else {
                newIndex["Unique"] = false;
            }
            newIndex["Keys"] = keysList;
            if (table.Indexes != null && table.Indexes.length > 0) {
                newIndexPos = table.Indexes.length;
                for (let x = 0; x < table.Indexes.length; x++) {
                    if (JSON.stringify(table.Indexes[x].Keys) === JSON.stringify(keysList)) {
                        Actions.hideSpinner()
                        showSnackbar("Index with selected key(s) already exists.\n Please use different key(s)", " redBg");
                        return;
                    } else if (newIndex["Name"] === table.Indexes[x].Name) {
                        Actions.hideSpinner()
                        showSnackbar("Index with name: " + newIndex["Name"] + " already exists.\n Please try with a different name", " redBg");
                        return;
                    }
                }
            } else {
                newIndexPos = 0;
            }
            let res = await Fetch.getAppData("POST", "/add/indexes?table=" + tableName, [newIndex]);
            if (res.ok) {
                jQuery("#createIndexModal").modal("hide");
                res = await res.json();
                Store.updatePrimaryKeys(res);
                Store.updateTableData("reportTabContent", res);
                Actions.resetReportTableData();
            } else {
                res = await res.text();
                Actions.hideSpinner()
                showSnackbar(res, " redBg");
            }
        },

        createNewSecIndex: (id) => {
            let iIndex = id.indexOf("indexButton");
            let tableIndex = id.substring(3, iIndex)
            let tableName = id.substring(iIndex + 12)
            let jsonObj = Store.getinstance().tableData.reportTabContent;
            if (document.getElementById("editSpanner" + tableIndex).innerHTML.trim() == "Save Changes") {
                let pendingChanges = false;
                let dataTable = jQuery(`#src-sp-table${tableIndex} tr`)
                dataTable.each(function(index) {
                    if (index > 1) {
                        let newColumnName;
                        let srcColumnName = jQuery(this).find('.src-column').html().trim();
                        let indexNumber = jQuery(this).find('.src-column').attr('id').match(/\d+/)[0];
                        indexNumber = indexNumber.substring(tableIndex.toString().length);
                        let indexNumberlength = indexNumber.length / 2;
                        indexNumber = indexNumber.substring(indexNumberlength);
                        let newColumnNameEle = document.getElementById('column-name-text-' + tableIndex + indexNumber + indexNumber);
                        if (newColumnNameEle) {
                            newColumnName = newColumnNameEle.value;
                        }
                        let oldColumnName = jsonObj.ToSpanner[tableName].Cols[srcColumnName];
                        if (newColumnName !== oldColumnName || !(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
                            let errorModal = document.querySelector("hb-modal[modalId = editTableWarningModal]");
                            let content = "There are pending changes to this table, please save the same before creating the index";
                            errorModal.setAttribute("content", content);
                            jQuery("#editTableWarningModal").modal();
                            pendingChanges = true;
                        }
                    }
                })
                if (pendingChanges) {
                    return;
                }
            }
            let generalModal = document.querySelector("hb-modal[modalId = createIndexModal]")
            const { SpSchema } = Store.getinstance().tableData.reportTabContent;
            let content = `<hb-add-index-form tableName=${tableName} 
      tableIndex=${tableIndex} coldata=${JSON.stringify(SpSchema[tableName].ColNames)}  ></hb-add-index-form>`;
            generalModal.setAttribute("content", content);
            jQuery("#createIndexModal").modal();
            resetIndexModal();
        },

        closeSecIndexModal: () => {
            resetIndexModal();
            let generalModal = document.querySelector("hb-modal[modalId = createIndexModal]");
            let content = `empty`;
            generalModal.setAttribute("content", content);
        },

        changeCheckBox: (row, id) => {
            let columnName = document.getElementById(`order${row}${id}`);
            let checkboxValue = document.getElementById("checkbox-" + row + "-" + id).checked;
            if (checkboxValue) {
                columnName.style.visibility = "visible";
                columnName.innerHTML = orderId + 1;
                orderId++;
                keysList.push({ Col: row, Desc: false });
                temp[row] = id;
            } else {
                columnName.style.visibility = "hidden";
                let oldValue = parseInt(columnName.innerHTML);
                for (let i = 0; i < keysList.length; i++) {
                    let currentRow = keysList[i].Col;
                    let currentId = temp[currentRow];
                    let currentColName = document.getElementById(`order${currentRow}${currentId}`);
                    if (parseInt(currentColName.innerHTML) > oldValue) {
                        currentColName.innerHTML = parseInt(currentColName.innerHTML) - 1;
                    }
                }
                keysList = keysList.filter((cur) => cur.Col !== row);
                temp[row] = -1;
                orderId--;
            }
        },

        SaveButtonHandler: async(tableNumber, tableName, notNullConstraint) => {
            var errorMessage = [];
            let schemaConversionObj = {...Store.getinstance().tableData.reportTabContent };
            let fkStatus = false,
                secIndexStatus = false,
                columnStatus = false;
            columnStatus = await Actions.saveColumn(schemaConversionObj, tableNumber, tableName, notNullConstraint, tableData, errorMessage);
            fkStatus = await Actions.saveForeignKeys(schemaConversionObj, tableNumber, tableName, tableData, errorMessage);
            secIndexStatus = await Actions.saveSecondaryIndexes(schemaConversionObj, tableNumber, tableName, tableData, errorMessage);
            if (fkStatus && secIndexStatus && columnStatus) {
                Store.updatePrimaryKeys(tableData.data);
                Store.updateTableData("reportTabContent", tableData.data);
                Actions.ddlSummaryAndConversionApiCall();
                Actions.resetReportTableData();
                Store.setTableMode(tableNumber, false);
            } else {
                Actions.hideSpinner();
                let message = errorMessage.map((msg, idx) => `<span class="primary-color-number"><b>${idx + 1}.</b></span> ${msg}`).join('<br/>')
                jQuery('#editTableWarningModal').modal();
                jQuery('#editTableWarningModal').find('#modal-content').html(`<div class="error-content-container">${message}<div>`);
            }
        },

        isValueUpdated: (data, tableNumber, tableName, notNullConstraint) => {
            let columnPerPage = 15;
            let tableId = '#src-sp-table' + tableNumber + ' tr';
            let pageNumber = Store.getCurrentPageNumber(tableNumber)
            let pageColArray = data.SpSchema[tableName].ColNames
                .filter((_, idx) => idx >= pageNumber * columnPerPage && idx < pageNumber * columnPerPage + columnPerPage);
            for (let i = 0; i < columnPerPage; i++) {
                let newName = document.getElementById('column-name-text-' + tableNumber + i + i).value;
                let newType = document.getElementById('data-type-' + tableNumber + i + i).value;
                let newConstraint = notNullConstraint[i] === 'Not Null';
                if (pageColArray[i] !== newName ||
                    newType !== data.SpSchema[tableName].ColDefs[pageColArray[i]].T.Name ||
                    data.SpSchema[tableName].ColDefs[pageColArray[i]].NotNull !== newConstraint) {
                    return true;
                }
            }
            let flagofcheckbox = false;
            jQuery(tableId).each(function(index) {
                if (!(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
                    flagofcheckbox = true;
                    return false;
                }
            })
            if (flagofcheckbox) {
                return true;
            }
            return false;
        },

        saveColumn: async(schemaConversionObj, tableNumber, tableName, notNullConstraint, tableData, errorMessage, updateInStore = false) => {
            let data;
            if (tableData.data.SpSchema != undefined) {
                data = {...tableData.data };
            } else {
                if (updateInStore) {
                    data = {...Store.getinstance().tableData.reportTabContent };
                } else {
                    data = {...schemaConversionObj };
                }
            }

            if (updateInStore && !Actions.isValueUpdated(data, tableNumber, tableName, notNullConstraint)) {
                return true;
            }
            let tableId = '#src-sp-table' + tableNumber + ' tr';
            let tableColumnNumber = 0;

            let columnNameExists = false;
            let columnNameEmpty = false;
            let columnStatus = false,
                duplicateInPage = false;

            let updatedColsData = {
                'UpdateCols': {}
            }
            let newColArrayForDuplicateCheck = [];
            let columnsNamesArray = [];
            // We follow this approach to create columnsNamesArray to ensure the order of column names in the array is the consistent with what is seen in the UI.
            // This is needed later on where we compare the array index with tableColumnNumber.
            jQuery(tableId).each(function(index) {
                if (index > 1) {
                    let srcColumnName = document.getElementById('src-column-name-' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML;
                    let originalColumnName = data.ToSpanner[tableName].Cols[srcColumnName];
                    columnsNamesArray.push(originalColumnName)
                    tableColumnNumber++;
                }
            });
            tableColumnNumber = 0;
            jQuery(tableId).each(function(index) {
                if (index > 1) {
                    let newColumnName;
                    let srcColumnName = document.getElementById('src-column-name-' + tableNumber + tableColumnNumber + tableColumnNumber).innerHTML;
                    let newColumnNameEle = document.getElementById('column-name-text-' + tableNumber + tableColumnNumber + tableColumnNumber);
                    if (newColumnNameEle) {
                        newColumnName = newColumnNameEle.value;
                        newColArrayForDuplicateCheck.push(newColumnName)
                    }
                    let originalColumnName = data.ToSpanner[tableName].Cols[srcColumnName];
                    updatedColsData.UpdateCols[originalColumnName] = {};
                    updatedColsData.UpdateCols[originalColumnName]['Removed'] = false;
                    if (newColumnName === originalColumnName) {
                        updatedColsData.UpdateCols[originalColumnName]['Rename'] = '';
                    } else if (newColumnName == "") {
                        errorMessage.push("Column name(s) cannot be empty");
                        columnNameEmpty = true;
                    } else {
                        columnNameExists = false;
                        for (let k = 0; k < columnsNamesArray.length; k++) {
                            if (k != tableColumnNumber && newColumnName === columnsNamesArray[k]) {
                                updatedColsData.UpdateCols[originalColumnName]['Rename'] = '';
                                columnNameExists = true;
                                errorMessage.push("Column : '" + newColumnName + "'" + ' already exists in table : ' + "'" + tableName + "'" + '. Please try with a different column name.')
                                break
                            }
                        }
                        if (!columnNameExists)
                            updatedColsData.UpdateCols[originalColumnName]['Rename'] = newColumnName;
                    }
                    updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'ADDED';
                    updatedColsData.UpdateCols[originalColumnName]['PK'] = '';
                    updatedColsData.UpdateCols[originalColumnName]['ToType'] = document.getElementById('data-type-' + tableNumber + tableColumnNumber + tableColumnNumber).value;
                    if (notNullConstraint[parseInt(String(tableNumber) + String(tableColumnNumber))] === 'Not Null') {
                        updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'ADDED';
                    } else if (notNullConstraint[parseInt(String(tableNumber) + String(tableColumnNumber))] === '') {
                        updatedColsData.UpdateCols[originalColumnName]['NotNull'] = 'REMOVED';
                    }
                    if (!(jQuery(this).find("input[type=checkbox]").is(":checked"))) {
                        updatedColsData.UpdateCols[originalColumnName]['Removed'] = true;
                    }
                    tableColumnNumber++;
                }
            });

            columnStatus = true;
            const s = new Set(newColArrayForDuplicateCheck);
            if (newColArrayForDuplicateCheck.length !== s.size) {
                duplicateInPage = true;
                errorMessage.push('Two column have same name in the current page.')
            }
            switch (columnNameExists || columnNameEmpty || duplicateInPage) {
                case true:
                    if (updateInStore) {
                        Actions.hideSpinner()
                        let message = errorMessage.map((msg, idx) => `<span class="primary-color-number"><b>${idx + 1}.</b></span> ${msg}`).join('<br/>')
                        jQuery('#editTableWarningModal').modal();
                        jQuery('#editTableWarningModal').find('#modal-content').html(`<div class="error-content-container">${message}<div>`);
                    }
                    return false;

                case false:
                    let fetchedTableData = await Fetch.getAppData('POST', '/typemap/table?table=' + tableName, updatedColsData);
                    if (fetchedTableData.ok) {
                        let tableDataTemp = await fetchedTableData.json();
                        if (updateInStore) {
                            Store.updatePrimaryKeys(tableDataTemp)
                            Store.updateTableData("reportTabContent", tableDataTemp);
                            Actions.resetReportTableData();
                        }
                        tableData.data = tableDataTemp;
                        let checkInterleave = Store.getinstance().checkInterleave;
                        if (checkInterleave[tableName]) {
                            let selectedValue;
                            let radioGroup = 'fks' + tableNumber;
                            let radioValues = document.querySelectorAll('input[name=' + radioGroup + ']');
                            for (const x of radioValues) {
                                if (x.checked) {
                                    selectedValue = x.value;
                                    break;
                                }
                            }
                            if (selectedValue == 'interleave') {
                                let response = await Fetch.getAppData('GET', '/setparent?table=' + tableName + '&update=' + true);
                                response = await response.json();
                                tableData.data = response.sessionState;
                            }
                        }
                    } else {
                        let modalData = await fetchedTableData.text();
                        errorMessage.push(modalData)
                        if (updateInStore) {
                            Actions.hideSpinner()
                            let message = errorMessage.map((msg, idx) => `<span class="primary-color-number"><b>${idx + 1}.</b></span> ${msg}`).join('<br/>')
                            jQuery('#editTableWarningModal').modal();
                            jQuery('#editTableWarningModal').find('#modal-content').html(`<div class="error-content-container">${message}<div>`);
                        }
                        return false;
                    }
            }
            return true;
        },

        saveForeignKeys: async(schemaConversionObj, tableNumber, tableName, tableData, errorMessage) => {
            let fkTableData, renameFkMap = {},
                fkLength;
            let uniquevals;
            let newFkValueArray = [];
            let data;
            if (tableData.data.SpSchema != undefined) {
                data = {...tableData.data };
            } else data = {...schemaConversionObj };
            if (data.SpSchema[tableName].Fks != null && data.SpSchema[tableName].Fks.length != 0) {
                fkLength = data.SpSchema[tableName].Fks.length;
                for (let x = 0; x < fkLength; x++) {
                    let newFkVal = document.getElementById('new-fk-val-' + tableNumber + x).value;
                    newFkValueArray.push(newFkVal)
                    if (data.SpSchema[tableName].Fks[x].Name != newFkVal)
                        renameFkMap[data.SpSchema[tableName].Fks[x].Name] = newFkVal;
                }
                uniquevals = [...new Set(newFkValueArray)];
                if (Object.keys(renameFkMap).length > 0) {
                    let duplicateCheck = [];
                    let duplicateFound = false;
                    let keys = Object.keys(renameFkMap);
                    let flag = false;
                    let dummyobj = {};
                    keys.forEach(function(key) {

                        for (let x = 0; x < fkLength; x++) {
                            if (data.SpSchema[tableName].Fks[x].Name === renameFkMap[key]) {
                                if (uniquevals.length == newFkValueArray.length) {
                                    flag = true;
                                    break;
                                } else {
                                    errorMessage.push("Foreign Key: " + renameFkMap[key] + " already exists in table: " + tableName + ". Please try with a different name.")
                                    duplicateFound = true;
                                    break;
                                }
                            }
                        }

                        if (duplicateCheck.includes(renameFkMap[key])) {
                            errorMessage.push('Please use a different name for each foreign key')

                            duplicateFound = true;
                        } else {
                            duplicateCheck.push(renameFkMap[key]);
                        }
                    });
                    if (flag) {
                        let dummyrenameFkMap = {};
                        keys.forEach(function(key) {
                            dummyobj[key] = new Date().toString() + key;
                        });
                        fkTableData = await Fetch.getAppData('POST', '/rename/fks?table=' + tableName, dummyobj);
                        if (fkTableData.ok) {
                            keys.forEach(function(key) {
                                dummyrenameFkMap[dummyobj[key]] = renameFkMap[key];
                            })
                        }
                        renameFkMap = dummyrenameFkMap;
                    }
                    switch (duplicateFound) {
                        case true:
                            return false;

                        case false:
                            fkTableData = await Fetch.getAppData('POST', '/rename/fks?table=' + tableName, renameFkMap);
                            if (!fkTableData.ok) {
                                fkTableData = await fkTableData.text();
                                errorMessage.push(fkTableData)
                                return false;
                            } else {
                                fkTableData = await fkTableData.json();
                                tableData.data = fkTableData;
                            }
                    }
                }
            }
            return true;
        },

        saveSecondaryIndexes: async(schemaConversionObj, tableNumber, tableName, tableData, errorMessage) => {
            let data;
            let newSecIndexArray = [];
            let uniquevals;
            let secIndexTableData, renameIndexMap = {},
                secIndexLength;

            if (tableData.data.SpSchema != undefined) {
                data = {...tableData.data };
            } else data = {...schemaConversionObj };

            if (data.SpSchema[tableName].Indexes != null && data.SpSchema[tableName].Indexes.length != 0) {
                secIndexLength = data.SpSchema[tableName].Indexes.length;
                for (let x = 0; x < secIndexLength; x++) {
                    let newSecIndexVal = document.getElementById('new-sec-index-val-' + tableNumber + x).value;
                    newSecIndexArray.push(newSecIndexVal)
                    if (data.SpSchema[tableName].Indexes[x].Name != newSecIndexVal)
                        renameIndexMap[data.SpSchema[tableName].Indexes[x].Name] = newSecIndexVal;
                }
                uniquevals = [...new Set(newSecIndexArray)];
                if (Object.keys(renameIndexMap).length > 0) {
                    let duplicateCheck = [];
                    let duplicateFound = false;
                    let keys = Object.keys(renameIndexMap);
                    let flag = false;
                    keys.forEach(function(key) {
                        for (let x = 0; x < secIndexLength; x++) {
                            if (data.SpSchema[tableName].Indexes[x].Name === renameIndexMap[key]) {
                                if (uniquevals.length == newSecIndexArray.length) {
                                    flag = true;
                                    break;
                                } else {

                                    errorMessage.push("Index: " + renameIndexMap[key] + " already exists in table: " + tableName + ". Please try with a different name.")

                                    duplicateFound = true;
                                    break;
                                }
                            }
                        }
                        if (duplicateCheck.includes(renameIndexMap[key])) {
                            errorMessage.push('Please use a different name for each secondary index')
                            duplicateFound = true;
                        } else {
                            duplicateCheck.push(renameIndexMap[key]);
                        }
                    });
                    if (flag) {
                        let dummyobj = {};
                        let dummyrenameSecIndexMap = {};
                        keys.forEach(function(key) {
                            dummyobj[key] = new Date().toString() + key;
                        });
                        secIndexTableData = await Fetch.getAppData('POST', '/rename/indexes?table=' + tableName, dummyobj);
                        if (secIndexTableData.ok) {
                            keys.forEach(function(key) {
                                dummyrenameSecIndexMap[dummyobj[key]] = renameIndexMap[key];
                            })
                        }
                        renameIndexMap = dummyrenameSecIndexMap;
                    }
                    switch (duplicateFound) {
                        case true:
                            return false;

                        case false:
                            secIndexTableData = await Fetch.getAppData('POST', '/rename/indexes?table=' + tableName, renameIndexMap);
                            if (!secIndexTableData.ok) {
                                secIndexTableData = await secIndexTableData.text();
                                errorMessage.push(secIndexTableData)
                                return false;
                            } else {
                                secIndexTableData = await secIndexTableData.json();
                                tableData.data = secIndexTableData;
                            }
                    }
                }
            }
            return true;
        },

        dropForeignKeyHandler: async(tableName, tableNumber, pos) => {
            let response;
            Actions.showSpinner();
            response = await Fetch.getAppData('GET', '/drop/fk?table=' + tableName + '&pos=' + pos);
            if (response.ok) {
                let responseCopy = response.clone();
                let jsonResponse = await responseCopy.json();
                Store.updatePrimaryKeys(jsonResponse);
                Store.updateTableData("reportTabContent", jsonResponse);
                Actions.resetReportTableData();

                if (jsonResponse.SpSchema[tableName].Fks === null && jsonResponse.SpSchema[tableName].Fks.length === 0) {
                    jQuery('#' + tableNumber).find('.fk-card').addClass('template');
                }
            }
        },

        dropSecondaryIndexHandler: async(tableName, tableNumber, pos) => {
            Actions.showSpinner()
            let response;
            response = await Fetch.getAppData('GET', '/drop/secondaryindex?table=' + tableName + '&pos=' + pos);
            if (response.ok) {
                let responseCopy = response.clone();
                let jsonObj = await responseCopy.json();
                Store.updatePrimaryKeys(jsonObj);
                Store.updateTableData("reportTabContent", jsonObj);
                Actions.resetReportTableData();
            }
        },

        showSpinner: () => {
            let toggle_spinner = document.getElementById("toggle-spinner");
            toggle_spinner.style.display = "block";
        },

        hideSpinner: () => {
            let toggle_spinner = document.getElementById("toggle-spinner");
            toggle_spinner.style.display = "none";
            toggle_spinner.className = toggle_spinner.className.replace("show", "");
        },

        switchCurrentTab: (tab) => {
            if (Store.getCurrentTab() !== tab) Actions.showSpinner()
            Store.switchCurrentTab(tab)
        },

        openCarousel: (tableId, tableIndex) => {
            Store.openCarousel(tableId, tableIndex)
        },

        closeCarousel: (tableId, tableIndex) => {
            Store.closeCarousel(tableId, tableIndex)
        },

        getTableData: (tabName) => {
            Store.getTableData(tabName);
        },

        getSearchInputValue: (key) => {
            return Store.getSearchInputValue(key);
        },

        getCurrentTab: () => {
            return Store.getCurrentTab();
        },

        setSourceDbName: (name) => {
            Store.setSourceDbName(name)
        },

        setGlobalDbType: (value) => {
            Store.setGlobalDbType(value);
        },

        getInterleaveConversionForATable: (tableName) => {
            return Store.getInterleaveConversionForATable(tableName);
        },

        getSourceDbName: () => {
            return Store.getSourceDbName();
        },

        getGlobalDataTypeList: () => {
            return Store.getGlobalDataTypeList();
        },

        carouselStatus: (tabId) => {
            return Store.getinstance().openStatus[tabId];
        },

        getTableMode: (tableIndex) => {
            return Store.getTableMode(tableIndex);
        },

        setTableMode: (tableIndex, val) => {
            Store.setTableMode(tableIndex, val);
        },
        incrementPageNumber: (tableIndex) => {
            Store.incrementPageNumber(tableIndex);
        },

        decrementPageNumber: (tableIndex) => {
            Store.decrementPageNumber(tableIndex);
        },

        getCurrentPageNumber: (idx) => {
            return Store.getCurrentPageNumber(idx)
        },

        changePage: (tableIndex, pageindex) => {
            if (Actions.getCurrentPageNumber(tableIndex) == pageindex) {
                Actions.hideSpinner();
            }
            Store.setPageNumber(tableIndex, pageindex);
        }


    };
})();

export default Actions;