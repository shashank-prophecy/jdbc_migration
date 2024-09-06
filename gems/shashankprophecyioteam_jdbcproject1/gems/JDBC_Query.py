from prophecy.cb.server.base.ComponentBuilderBase import *
from pyspark.sql import *
from pyspark.sql.functions import *
from prophecy.cb.ui.uispec import *
from prophecy.cb.server.base.WorkflowContext import WorkflowContext
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
                        RadioGroup("Credentials")
                            .addOption("Username & Password", "userPwd")
                            .addOption("Environment variables", "userPwdEnv")
                            .bindProperty("credType")
                    )
                        .addElement(
                        Condition()
                            .ifEqual(
                            PropExpr("component.properties.credType"),
                            StringExpr("userPwd"),
                        )
                            .then(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(
                                TextBox("Username")
                                    .bindPlaceholder("username")
                                    .bindProperty("textUsername")
                            )
                                .addColumn(
                                TextBox("Password")
                                    .isPassword()
                                    .bindPlaceholder("password")
                                    .bindProperty("textPassword")
                            )
                        )
                            .otherwise(
                            ColumnsLayout(gap=("1rem"))
                                .addColumn(
                                TextBox("Username")
                                    .bindPlaceholder("username")
                                    .bindProperty("textUsername")
                            )
                                .addColumn(
                                TextBox("Password")
                                    .bindPlaceholder("password")
                                    .bindProperty("textPassword")
                            )
                        )
                    )
                )
                    .addElement(TitleElement(title="URL"))
                    .addElement(
                    TextBox("JDBC URL")
                        .bindPlaceholder(
                        "jdbcHostname"
                    )
                        .bindProperty("jdbcUrl")
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
    def validate(self, context: WorkflowContext, component: Component[JDBC_QueryProperties]) -> List[Diagnostic]:
        diagnostics = []
        if component.properties.textUsername is None:
            diagnostics.append(
                Diagnostic(
                    "properties.textUsername",
                    "Username cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.textPassword is None:
            diagnostics.append(
                Diagnostic(
                    "properties.textPassword",
                    "Password cannot be empty",
                    SeverityLevelEnum.Error,
                )
            )
        if component.properties.jdbcUrl is None or component.properties.jdbcUrl == "":
            diagnostics.append(
                Diagnostic(
                    "properties.jdbcUrl",
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
    def onChange(
            self,
context: WorkflowContext,             oldState: Component[JDBC_QueryProperties],
            newState: Component[JDBC_QueryProperties],
    ) -> Component[JDBC_QueryProperties]:
        return newState
    class JDBC_QueryCode(ComponentCode):
        def __init__(self, newProps):
            self.props: JDBC_Query.JDBC_QueryProperties = newProps
        def apply(self, spark: SparkSession):
            import pymysql
            dbUser = self.props.textUsername
            dbPassword = self.props.textPassword
            if self.props.credType == "userPwdEnv":
                import os
                dbUser = os.environ[self.props.textUsername]
                dbPassword = os.environ[self.props.textPassword]
            if self.props.connect_timeout is not None:
                connectionObject: SubstituteDisabled = pymysql.connect(
                    host=self.props.jdbcUrl,
                    user=dbUser,
                    password=dbPassword,
                    db=self.props.databaseName,
                    connect_timeout=int(self.props.connect_timeout),
                )
            else:
                connectionObject: SubstituteDisabled = pymysql.connect(
                    host=self.props.jdbcUrl,
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