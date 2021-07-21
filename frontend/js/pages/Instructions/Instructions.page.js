import { setActiveSelectedMenu } from './../../helpers/SchemaConversionHelper.js';

class Instructions extends HTMLElement {

    connectedCallback() {
        this.render();
        setActiveSelectedMenu('instructions');
    }

    render() {
        this.innerHTML = `
            <div class="instructions-div">
                <img src='Icons/Icons/google-spanner-logo.png' class="instructions-icon" />
                <h1 class="textCenter">HarbourBridge User Manual</h1>
                <br><br>
                <h3 class="instructions-main-heading">1 &nbsp; &nbsp; &nbsp;Introduction</h3>
                <p>HarbourBridge is a stand-alone open source tool for Cloud Spanner evaluation, using data from an existing
                    PostgreSQL or MySQL database. The tool ingests schema and data from either a pg_dump/mysqldump file or directly
                    from the source database, automatically builds a Spanner schema, and creates a new Spanner database
                    populated with data from the source database.
                    <br><br>
                    HarbourBridge is designed to simplify Spanner evaluation, and in particular to bootstrap the process by getting
                    moderate-size PostgreSQL/MySQL datasets into Spanner (up to a few tens of GB). Many features of
                    PostgreSQL/MySQL,
                    especially those that don't map directly to Spanner features, are ignored, e.g. (non-primary) indexes,
                    functions,
                    and sequences. Types such as integers, floats, char/text, bools, timestamps, and (some) array types, map fairly
                    directly to Spanner, but many other types do not and instead are mapped to Spanner's STRING(MAX).
                </p>
                <br>
                <h4 class="instructions-sub-heading">1.1 &nbsp; &nbsp; &nbsp;HarbourBridge UI</h4>
                <p>HarbourBridge UI is designed to focus on generating spanner schema from either a pg_dump/mysqldump file or
                    directly from the source database and providing edit functionality to the spanner schema and thereby creating a
                    new spanner database populated with new data. UI gives the provision to edit column name, edit data type, edit
                    constraints, drop foreign key and drop secondary index of spanner schema.</p>
                <br>
                <h3 class="instructions-main-heading">2 &nbsp; &nbsp; &nbsp;Key Features of UI</h3>
                <ul>
                    <li> Connecting to a new database</li>
                    <li> Load dump file</li>
                    <li> Load session file</li>
                    <li> Storing session for each conversion</li>
                    <li> Edit data type globally for each table in schema</li>
                    <li> Edit data type, column name, constraint for a particular table</li>
                    <li> Edit foreign key and secondary index name</li>
                    <li> Drop a column from a table</li>
                    <li> Drop foreign key from a table</li>
                    <li> Drop secondary index from a table</li>
                    <li> Convert foreign key into interleave table</li>
                    <li> Search a table</li>
                    <li> Download schema, report and session files</li>
                </ul>
                <br>

                <h3 class="instructions-main-heading">3 &nbsp; &nbsp; &nbsp;UI Setup</h3>
                <ul>
                    <li> Install go in local</li>
                    <li> Clone harbourbridge project and run following command in the terminal: <br>
                        <span class="instructions-command">go run main.go --web</span>
                    </li>
                    <li> Open <span class="instructions-command">http://localhost:8080</span>in browser</li>
                </ul>
                <br>

                <h3 class="instructions-main-heading">4 &nbsp; &nbsp; &nbsp;Different modes to select
                    source database</h3>
                <h4 class="instructions-sub-heading">4.1 &nbsp; &nbsp; &nbsp;Connect to Database</h4>
                <ul>
                    <li> Enter database details in connect to database dialog box</li>
                    <li> Input Fields: database type, database host, database port, database user, database name, database password
                    </li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/connectToDb.png'>
                <br><br><br><br><br><br>
                <img class="instructions-img-width" src='userManualImages/connectToDbWithOptions.png'>
                <br><br><br>

                <h4 class="instructions-sub-heading">4.2 &nbsp; &nbsp; &nbsp;Load Database Dump</h4>
                <ul>
                    <li> Enter dump file path in load database dialog box</li>
                    <li> Input Fields: database type, file path</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/loadDumpFile.png'>
                <br><br><br>

                <h4 class="instructions-sub-heading">4.3 &nbsp; &nbsp; &nbsp;Import Schema File</h4>
                <ul>
                    <li> Enter session file path in load session dialog box</li>
                    <li> Input Fields: database type, session file path</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/loadSessionFile.png'>
                <br><br><br>

                <h3 class="instructions-main-heading">5 &nbsp; &nbsp; &nbsp;Session Table</h3>
                <ul>
                    <li> Session table is used to store the previous sessions of schema conversion</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/sessionTable.png'>
                <br><br><br>

                <h3 class="instructions-main-heading">6 &nbsp; &nbsp; &nbsp;Edit Global Data Type</h3>
                <ul>
                    <li> Click on edit global data type button on the screen</li>
                    <li> Select required spanner data type from the dropdown available for each source data type</li>
                    <li> Click on next button after making all the changes</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/globalDataTypeMapping.png'>
                <br><br><br>

                <h3 class="instructions-main-heading">7 &nbsp; &nbsp; &nbsp;Edit Spanner Schema for a
                    particular table</h3>
                <ul>
                    <li> Expand any table</li>
                    <li> Click on edit spanner schema button</li>
                    <li> Edit column name/ data type/ constraint of spanner schema</li>
                    <li> Edit name of secondary index or foreign key</li>
                    <li> Select to convert foreign key to interleave or use as is (if option is available)</li>
                    <li> Drop a column by unselecting any checkbox</li>
                    <li> Drop a foreign key or secondary index by expanding foreign keys or secondary indexes tab inside table</li>
                    <li> Click on save changes button to save the changes</li>
                    <li> If current table is involved in foreign key/secondary indexes relationship with other table then user will
                        be prompt to delete foreign key or secondary indexes and then proceed with save changes</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/editButtonClicked.png'>
                <br><br><br>
                <p>- Warning before deleting secondary index from a table</p>
                <img class="instructions-img-width" src='userManualImages/warningSecIndexDeletion.png'>
                <br><br><br>
                <p>- Error on saving changes</p>
                <img class="instructions-img-width" src='userManualImages/errorSaveChanges.png'>
                <br><br><br>
                <p>- Changes saved successfully after resolving all errors</p>
                <img class="instructions-img-width" src='userManualImages/successSaveChanges.png'>
                <br><br><br><br>

                <h3 class="instructions-main-heading">8 &nbsp; &nbsp; &nbsp;Download Session File</h3>
                <ul>
                    <li> Save all the changes done in spanner schema table wise or globally</li>
                    <li> Click on download session file button on the top right corner</li>
                    <li> Save the generated session file with all the changes in local machine</li>
                </ul>
                <br>
                <img class="instructions-img-width" src='userManualImages/downloadSessionFile.png'>
                <br><br><br>

                <h3 class="instructions-main-heading">9 &nbsp; &nbsp; &nbsp;How to use Session File
                </h3>
                <p>Please refer below link to get more information on how to use session file with harbourbridge tool</p>
                <a href='https://github.com/cloudspannerecosystem/harbourbridge' class="instructionsLink">Refer this to use Session File</a>
                <br>
            </div>
        `;
    }

    constructor() {
        super();
    }
}

window.customElements.define('hb-instructions', Instructions);