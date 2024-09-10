from prophecy.cb.server.base.ComponentBuilderBase import *
from prophecy.cb.server.base.DatasetBuilderBase import DatasetSpec, DatasetProperties, Component
from pyspark.sql import *
from pyspark.sql.functions import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
from prophecy.cb.migration import PropertyMigrationObj

class JDBC_Query(ComponentSpec):
    name: str = "JDBC_Query"
    category: str = "Custom"
    def optimizeCode(self) -> bool:
        return True
    @dataclass(frozen=True)
    class JDBC_QueryProperties(ComponentProperties):
        description: Optional[str] = ""
        credType: str = "userPwdEnv"
        textUsername: Optional[str] = None
        textPassword: Optional[str] = None
        jdbcUrl: str = ""
        secretUsername: SecretValue = field(default_factory=list)
        secretPassword: SecretValue = field(default_factory=list)
        secretJdbcUrl: SecretValue = field(default_factory=list)
        databaseName: Optional[str] = None
        dbtable: Optional[str] = None
        query: Optional[str] = None
        connect_timeout: Optional[str] = None
    def dialog(self) -> Dialog:
        return Dialog("JDBC_Query").addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(PortSchemaTabs().importSchema(), "2fr")
                .addColumn(
                StackLayout(direction=("vertical"), gap=("1rem"))
                    .addElement(
                    StackLayout()
                        .addElement(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(
                                SecretBox("Username")
                                    .bindPlaceholder("username")
                                    .bindProperty("secretUsername")
                            )
                                .addColumn(
                                SecretBox("Password")
                                    .isPassword()
                                    .bindPlaceholder("password")
                                    .bindProperty("secretPassword")
                            )
                    )
                )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    SecretBox("JDBC URL")
                        .bindPlaceholder(
                        "jdbcHostname"
                    )
                        .bindProperty("secretJdbcUrl")
                )
                    .addElement(
                    StackLayout()
                        .addElement(
                        TextBox("Database Name")
                            .bindPlaceholder("database")
                            .bindProperty("databaseName")
                    )
                        .addElement(
                        TextBox("SQL Query")
                            .bindPlaceholder("truncate table table_to_truncate")
                            .bindProperty("query")
                    )
                        .addElement(
                        TextBox("Connection Timeout in s (Optional)")
                            .bindPlaceholder("20")
                            .bindProperty("connect_timeout")
                    )
                ),
                "5fr",
            )
        )
    def validate(self, context: WorkflowContext, component: Component) -> list:
        diagnostics = []
        if not component.properties.secretUsername.parts:
            diagnostics.append(
                Diagnostic(
                    "properties.secretUsername",
                    "Username cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if not component.properties.secretPassword.parts:
            diagnostics.append(
                Diagnostic(
                    "properties.secretPassword",
                    "Password cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if not component.properties.secretJdbcUrl.parts:
            diagnostics.append(
                Diagnostic(
                    "properties.secretJdbcUrl",
                    "JDBC URL cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.databaseName is None:
            diagnostics.append(
                Diagnostic(
                    "properties.databaseName",
                    "Database name cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.query is None:
            diagnostics.append(
                Diagnostic(
                    "properties.query", "Query cannot be empty", SeverityLevelEnum.Error
                )
            )
        return diagnostics
    
    def onChange( self, context: WorkflowContext, oldState: Component, newState: Component) -> Component:
        return newState
    
    class JDBC_QueryCode(ComponentCode):
        def __init__(self, newProps):
            self.props: JDBC_Query.JDBC_QueryProperties = newProps
        def apply(self, spark: SparkSession):
            import pymysql
            dbUser = self.props.secretUsername
            dbPassword = self.props.secretPassword
            if self.props.credType == "userPwdEnv":
                import os
                dbUser = os.environ[self.props.secretUsername]
                dbPassword = os.environ[self.props.secretPassword]
            if self.props.connect_timeout is not None:
                connectionObject: SubstituteDisabled = pymysql.connect(
                    host=self.props.secretJdbcUrl,
                    user=dbUser,
                    password=dbPassword,
                    db=self.props.databaseName,
                    connect_timeout=int(self.props.connect_timeout),
                )
            else:
                connectionObject: SubstituteDisabled = pymysql.connect(
                    host=self.props.secretJdbcUrl,
                    user=dbUser,
                    password=dbPassword,
                    db=self.props.databaseName,
                )
            try:
                cursorObject = connectionObject.cursor()
                cursorObject.execute(f"{self.props.query}")
                connectionObject.commit()
            except Exception as e:
                print(str(e))
                raise e
            finally:
                connectionObject.close()

    def __init__(self):
        super().__init__()
        self.registerPropertyEvolution(JDBC_QueryCode_PropertyMigration())

class JDBC_QueryCode_PropertyMigration(PropertyMigrationObj):

    def migrationNumber(self) -> int:
        return 1

    def up(self, old_properties: JDBC_Query.JDBC_QueryProperties) -> JDBC_Query.JDBC_QueryProperties:
        credType = old_properties.credType

        if credType == "userPwd":
            creds = (SecretValuePart.convertTextToSecret(old_properties.textUsername),
                     SecretValuePart.convertTextToSecret(old_properties.textPassword))
        elif credType == "userPwdEnv":
            creds = ([VaultSecret("Environment", "", "0", None, old_properties.textUsername)],
                     [VaultSecret("Environment", "", "0", None, old_properties.textPassword)])
        else:
            raise Exception("Invalid credType:" + credType)

        return dataclasses.replace(
            old_properties,
            credType="",
            textUsername=None,
            textPassword=None,
            jdbcUrl="",
            secretUsername=SecretValue(creds[0]),
            secretPassword=SecretValue(creds[1]),
            secretJdbcUrl=SecretValue(SecretValuePart.convertTextToSecret(old_properties.jdbcUrl))
        )

    def down(self, new_properties: JDBC_Query.JDBC_QueryProperties) -> JDBC_Query.JDBC_QueryProperties:
        raise Exception("Downgrade is not implemented for this JDBC version")