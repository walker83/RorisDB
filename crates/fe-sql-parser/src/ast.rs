#[derive(Debug, Clone)]
pub enum Statement {
    Query(QueryStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    CreateDatabase(CreateDatabaseStmt),
    CreateTable(CreateTableStmt),
    CreateView {
        database: Option<String>,
        name: String,
        if_not_exists: bool,
        query: String,
        columns: Vec<String>,
    },
    CreateMaterializedView(CreateMaterializedViewStmt),
    DropDatabase(DropDatabaseStmt),
    DropTable(DropTableStmt),
    DropMaterializedView(DropMaterializedViewStmt),
    AlterMaterializedView(AlterMaterializedViewStmt),
    RefreshMaterializedView(RefreshMaterializedViewStmt),
    AlterTable(AlterTableStmt),
    TruncateTable {
        database: Option<String>,
        table: String,
        if_exists: bool,
    },
    ShowDatabases,
    ShowTables(Option<String>, Option<String>, bool),
    ShowCreateTable(String, String),
    ShowCreateDatabase(String),
    ShowCreateView(String, String),
    ShowPartitions(String, String),
    ShowTableStatus(Option<String>),
    ShowVariables {
        global: bool,
        pattern: Option<String>,
    },
    ShowProcesslist(bool),
    ShowIndex(String, String),
    ShowAlterTable(Option<String>),
    ShowBackends,
    ShowFrontends,
    ShowTableId,
    ShowPartitionId,
    ShowDynamicPartitionTables,
    ShowView(String, String),
    ShowCreateMaterializedView(String),
    Describe(String, String),
    Explain(ExplainStmt),
    UseDatabase(String),
    SetVariable(SetVariableStmt),
    Union(UnionStmt),
    CreateRepository(CreateRepositoryStmt),
    DropRepository(DropRepositoryStmt),
    ShowRepositories,
    BackupDatabase(BackupDatabaseStmt),
    RestoreDatabase(RestoreDatabaseStmt),
    ShowUsers,
    CreateUser(CreateUserStmt),
    DropUser(DropUserStmt),
    CreateCatalog(CreateCatalogStmt),
    DropCatalog(DropCatalogStmt),
    ShowCatalogs,
    RefreshCatalog(RefreshCatalogStmt),
    // Batch 2 DDL
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    CancelAlterTable(CancelAlterTableStmt),
    AlterColocateGroup(AlterColocateGroupStmt),
    AlterDatabase(AlterDatabaseStmt),
    DropView(DropViewStmt),
    AlterView(AlterViewStmt),
    // Batch 3/4 generic statements (parsed but simplified execution)
    ExportTable(ExportTableStmt),
    CancelExport(String),
    ShowExport,
    CreateFunction(CreateFunctionStmt),
    DropFunction(DropFunctionStmt),
    ShowFunctions(Option<String>),
    ShowCreateFunction(String),
    DescribeFunction(String),
    AnalyzeTable(AnalyzeTableStmt),
    DropStats(DropStatsStmt),
    ShowAnalyze(Option<String>),
    ShowStats(String),
    ShowTableStats(String),
    CreateJob(CreateJobStmt),
    DropJob(String),
    PauseJob(String),
    ResumeJob(String),
    CancelTask(String),
    InstallPlugin(InstallPluginStmt),
    UninstallPlugin(String),
    ShowPlugins,
    RecoverDatabase(String),
    RecoverTable {
        database: String,
        table: String,
    },
    RecoverPartition {
        database: String,
        table: String,
        partition: String,
    },
    DropCatalogRecycleBin(Option<String>),
    ShowCatalogRecycleBin,
    CreateSqlBlockRule(CreateSqlBlockRuleStmt),
    AlterSqlBlockRule(String, Vec<(String, String)>),
    DropSqlBlockRule(String),
    ShowSqlBlockRule(Option<String>),
    CreateRowPolicy(CreateRowPolicyStmt),
    DropRowPolicy {
        name: String,
        database: Option<String>,
        table: String,
    },
    ShowRowPolicy(Option<String>),
    KillAnalyzeJob(String),
    AlterStats(String, Vec<(String, String)>),

    // Transaction statements
    StartTransaction,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackTo(String),
    ReleaseSavepoint(String),
    SetTransactionIsolation(String),

    // Admin and operations statements
    ShowStatus {
        global: bool,
        pattern: Option<String>,
    },
    ShowEngines,
    ShowCharset,
    ShowCollation {
        pattern: Option<String>,
    },
    KillQuery(u64),
    KillConnection(u64),
    AdminCheckTable(String),
    AdminShowReplica,
}

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table: String,
    pub set_clauses: Vec<SetClause>,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct SetClause {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    /// Tables to delete from (multi-table DELETE)
    pub tables: Vec<String>,
    /// FROM clause with table refs and joins
    pub from: Option<TableRef>,
    /// USING clause (for MySQL DELETE ... USING syntax)
    pub using: Option<TableRef>,
    pub selection: Option<Expr>,
    /// ORDER BY clause (MySQL extension)
    pub order_by: Vec<OrderByItem>,
    /// LIMIT clause (MySQL extension)
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct QueryStmt {
    pub select_list: Vec<SelectItem>,
    pub from: Option<TableRef>,
    pub r#where: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub with: Option<Cte>,
    pub set_op: Option<SetOperation>,
}

#[derive(Debug, Clone)]
pub struct SetOperation {
    pub op: UnionOperator,
    pub all: bool,
    pub right: Box<QueryStmt>,
}

#[derive(Debug, Clone)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Box<QueryStmt>,
}

#[derive(Debug, Clone)]
pub struct UnionStmt {
    pub op: UnionOperator,
    pub all: bool,
    pub left: Box<QueryStmt>,
    pub right: Box<QueryStmt>,
}

#[derive(Debug, Clone, Copy)]
pub enum UnionOperator {
    Union,
    Except,
    Intersect,
}

#[derive(Debug, Clone)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum TableRef {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        r#type: JoinType,
        condition: Option<Expr>,
    },
    Subquery {
        query: Box<QueryStmt>,
        alias: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
}

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expr>>,
    pub query: Option<QueryStmt>,
    pub is_overwrite: bool,
    /// ON DUPLICATE KEY UPDATE assignments
    pub on_duplicate_key_update: Vec<OnDuplicateKeyUpdate>,
}

/// ON DUPLICATE KEY UPDATE clause
#[derive(Debug, Clone)]
pub struct OnDuplicateKeyUpdate {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct CreateDatabaseStmt {
    pub name: String,
    pub if_not_exists: bool,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub database: Option<String>,
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub keys_type: KeysType,
    pub unique_keys: Vec<UniqueKeyDef>,
    pub partition: Option<PartitionDef>,
    pub distribution: Option<DistributionDef>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<Expr>,
    pub agg_type: Option<String>,
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum KeysType {
    Duplicate,
    Aggregate,
    Unique,
    Primary,
}

#[derive(Debug, Clone)]
pub struct UniqueKeyDef {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PartitionDef {
    pub partition_type: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DistributionDef {
    pub dist_type: String,
    pub columns: Vec<String>,
    pub buckets: usize,
}

#[derive(Debug, Clone)]
pub struct DropDatabaseStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub database: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub database: Option<String>,
    pub table: String,
    pub operations: Vec<AlterOperation>,
}

#[derive(Debug, Clone)]
pub enum AlterOperation {
    AddColumn(ColumnDef),
    DropColumn(String),
    ModifyColumn(ColumnDef),
    RenameTable(String),
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    SetComment(String),
    SetProperty(Vec<(String, String)>),
    AddPartition {
        partition_name: String,
        values_less_than: Vec<String>,
        properties: Vec<(String, String)>,
    },
    DropPartition {
        partition_name: String,
        if_exists: bool,
        force: bool,
    },
    AddRollup {
        rollup_name: String,
        columns: Vec<String>,
        properties: Vec<(String, String)>,
    },
    DropRollup {
        rollup_name: String,
        if_exists: bool,
    },
    Replace {
        old_table: String,
        swap: bool,
        properties: Vec<(String, String)>,
    },
    AddGeneratedColumn(ColumnDef),
}

#[derive(Debug, Clone)]
pub struct ExplainStmt {
    pub statement: Box<Statement>,
    pub verbose: bool,
}

#[derive(Debug, Clone)]
pub struct SetVariableStmt {
    pub variable: String,
    pub value: Expr,
    pub is_global: bool,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub struct WhenThen {
    pub when: Expr,
    pub then: Expr,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Literal(LiteralValue),
    ColumnRef {
        table: Option<String>,
        column: String,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    FunctionCall {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        query: Box<QueryStmt>,
        negated: bool,
    },
    Exists(Box<QueryStmt>),
    Subquery(Box<QueryStmt>),
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    Cast {
        expr: Box<Expr>,
        target_type: String,
    },
    CaseWhen {
        cases: Vec<WhenThen>,
        else_expr: Option<Box<Expr>>,
    },
    Wildcard,
    /// DEFAULT keyword - represents the default value for a column
    Default,
}

#[derive(Debug, Clone)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    Date(String),
}

#[derive(Debug, Clone, Copy)]
pub enum BinaryOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Like,
    NotLike,
    In,
    NotIn,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

#[derive(Debug, Clone, Copy)]
pub enum UnaryOp {
    Not,
    Negate,
}

#[derive(Debug, Clone)]
pub struct CreateRepositoryStmt {
    pub name: String,
    pub repo_type: RepositoryType,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum RepositoryType {
    Local,
    S3,
    Hdfs,
}

#[derive(Debug, Clone)]
pub struct DropRepositoryStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct BackupDatabaseStmt {
    pub database: String,
    pub repository: String,
    pub backup_name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct RestoreDatabaseStmt {
    pub database: String,
    pub repository: String,
    pub backup_name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateMaterializedViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub if_not_exists: bool,
    pub query: String,
    pub columns: Vec<String>,
    pub refresh: Option<RefreshClause>,
}

#[derive(Debug, Clone)]
pub struct RefreshClause {
    pub r#type: RefreshType,
    pub concurrency: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub enum RefreshType {
    Complete,
    Fast,
}

#[derive(Debug, Clone)]
pub struct DropMaterializedViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterMaterializedViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub operation: AlterMaterializedViewOperation,
}

#[derive(Debug, Clone)]
pub enum AlterMaterializedViewOperation {
    PauseRefresh,
    ResumeRefresh,
    Rename(String),
}

#[derive(Debug, Clone)]
pub struct RefreshMaterializedViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub refresh_type: RefreshType,
}

#[derive(Debug, Clone)]
pub struct CreateUserStmt {
    pub username: String,
    pub hostname: Option<String>,
    pub auth_plugin: String,
    pub password: Option<String>,
    pub identified_by_password: bool,
    pub roles: Vec<String>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropUserStmt {
    pub username: String,
    pub hostname: Option<String>,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateCatalogStmt {
    pub name: String,
    pub catalog_type: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DropCatalogStmt {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct RefreshCatalogStmt {
    pub name: String,
}

// Batch 2 DDL types

#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub database: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: Option<String>,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DropIndexStmt {
    pub index_name: String,
    pub database: Option<String>,
    pub table: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CancelAlterTableStmt {
    pub database: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct AlterColocateGroupStmt {
    pub group_name: String,
    pub operation: ColocateGroupOperation,
}

#[derive(Debug, Clone)]
pub enum ColocateGroupOperation {
    AddTable {
        database: Option<String>,
        table: String,
    },
    RemoveTable {
        database: Option<String>,
        table: String,
    },
    SetProperty(Vec<(String, String)>),
}

#[derive(Debug, Clone)]
pub struct AlterDatabaseStmt {
    pub name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DropViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterViewStmt {
    pub database: Option<String>,
    pub name: String,
    pub query: String,
}

// Batch 3/4 types

#[derive(Debug, Clone)]
pub struct ExportTableStmt {
    pub database: Option<String>,
    pub table: String,
    pub path: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateFunctionStmt {
    pub name: String,
    pub args: Vec<String>,
    pub returns: Option<String>,
    pub properties: Vec<(String, String)>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropFunctionStmt {
    pub name: String,
    pub args: Vec<String>,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AnalyzeTableStmt {
    pub database: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub sample_rate: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct DropStatsStmt {
    pub database: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CreateJobStmt {
    pub name: String,
    pub schedule: String,
    pub execute: String,
}

#[derive(Debug, Clone)]
pub struct InstallPluginStmt {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct CreateSqlBlockRuleStmt {
    pub name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct CreateRowPolicyStmt {
    pub name: String,
    pub database: Option<String>,
    pub table: String,
    pub policy_type: String,
    pub using_expr: String,
}
