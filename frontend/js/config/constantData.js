export const NAVLINKS = {
    logo: {
        css: { nav: "navbar navbar-static-top", img: "logo" },
        img: { src: "../../../Icons/Icons/google-spanner-logo.png" },
    },

    links: [{
            text: "Home",
            href: "#/",
            aTagId: "homeScreen",
            name: "headerMenu",
        },

        {
            text: "Schema Conversion",
            href: "javascript:;",
            aTagId: "schemaScreen",
            name: "headerMenu",
        },

        {
            text: "Instructions",
            href: "#/instructions",
            aTagId: "instructions",
            name: "headerMenu",
        },
    ],
};

export const CLASS_NAMES = {
    heading: 'heading',
    subHeading: 'sub-heading',
    sessionHeading: 'session-heading',
    text: 'text'
}

export const MODALCONFIGS = {
    CONNECT_TO_DB_MODAL_BUTTONS: [{ value: "Connect", id: "connect-button", disabledProp: "disabled" }],
    LOAD_DB_DUMP_MODAL_BUTTONS: [{ value: "Confirm", id: "load-connect-button", disabledProp: "disabled", modalDismiss: true }],
    LOAD_SESSION_MODAL_BUTTONS: [{ value: "Confirm", id: "load-session-button", disabledProp: "disabled", modalDismiss: true }],
    CONNECTION_SUCCESS_MODAL: [{ value: "Convert", id: "convert-button", disabledProp: "" }],
    CONNECTION_FAILURE_MODAL: [{ value: "Ok", id: "connection-failure-button", disabledProp: "" }],
    EDIT_GLOBAL_DATATYPE_MODAL: [{ value: "Next", id: "data-type-button", disabledProp: "" }],
    EDIT_TABLE_WARNING_MODAL: [{ value: "Ok", id: "edit-table-warning", disabledProp: "" }],
    ADD_INDEX_MODAL: [{ value: "CREATE", id: "create-index-button", disabledProp: "disabled", modalDismiss: true }],
    FK_DROP_WARNING_MODAL: [{ value: "Yes", id: "fk-drop-confirm", disabledProp: "" }, { value: "No", id: "fk-drop-cancel", disabledProp: "" }],
}

export const MAIN_PAGE_ICONS = [{
        image: "Icons/Icons/Group 2048.svg",
        imageAltText: "connect to db",
        label: "Connect to Database",
        action: "openConnectionModal",
        modalDataTarget: "#connectToDbModal",
    },

    {
        image: "Icons/Icons/Group 2049.svg",
        imageAltText: "load database image",
        label: "Load Database Dump",
        action: "openDumpLoadingModal",
        modalDataTarget: "#loadDatabaseDumpModal",
    },

    {
        image: "Icons/Icons/importIcon2.jpg",
        imageAltText: "Import schema image",
        label: "Load Session File",
        action: "openSessionFileLoadModal",
        modalDataTarget: "#loadSchemaModal",
    },
];

export const MAIN_PAGE_STATIC_CONTENT = {
    HEADING_TEXT: "Welcome To HarborBridge",
    SUB_HEADING_TEXT: "Connect or import your database",
    CONNECTION_SUCCESS_CONTENT: "Please click on convert button to proceed with schema conversion",
    CONNECTION_FAILURE_CONTENT: "Please check database configuration details and try again !!",
}

export const TAB_CONFIG_DATA = [{
        id: "report",
        text: "Schema",
    },

    {
        id: "ddl",
        text: "DDL",
    },

    {
        id: "summary",
        text: "Warnings",
    },
];

export const HISTORY_TABLE_HEADING = "Conversion history";