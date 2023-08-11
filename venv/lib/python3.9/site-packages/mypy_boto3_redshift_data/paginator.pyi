"""
Type annotations for redshift-data service client paginators.

[Open documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/)

Usage::

    ```python
    from boto3.session import Session

    from mypy_boto3_redshift_data.client import RedshiftDataAPIServiceClient
    from mypy_boto3_redshift_data.paginator import (
        DescribeTablePaginator,
        GetStatementResultPaginator,
        ListDatabasesPaginator,
        ListSchemasPaginator,
        ListStatementsPaginator,
        ListTablesPaginator,
    )

    session = Session()
    client: RedshiftDataAPIServiceClient = session.client("redshift-data")

    describe_table_paginator: DescribeTablePaginator = client.get_paginator("describe_table")
    get_statement_result_paginator: GetStatementResultPaginator = client.get_paginator("get_statement_result")
    list_databases_paginator: ListDatabasesPaginator = client.get_paginator("list_databases")
    list_schemas_paginator: ListSchemasPaginator = client.get_paginator("list_schemas")
    list_statements_paginator: ListStatementsPaginator = client.get_paginator("list_statements")
    list_tables_paginator: ListTablesPaginator = client.get_paginator("list_tables")
    ```
"""
from typing import Generic, Iterator, TypeVar

from botocore.paginate import PageIterator, Paginator

from .literals import StatusStringType
from .type_defs import (
    DescribeTableResponseTypeDef,
    GetStatementResultResponseTypeDef,
    ListDatabasesResponseTypeDef,
    ListSchemasResponseTypeDef,
    ListStatementsResponseTypeDef,
    ListTablesResponseTypeDef,
    PaginatorConfigTypeDef,
)

__all__ = (
    "DescribeTablePaginator",
    "GetStatementResultPaginator",
    "ListDatabasesPaginator",
    "ListSchemasPaginator",
    "ListStatementsPaginator",
    "ListTablesPaginator",
)

_ItemTypeDef = TypeVar("_ItemTypeDef")

class _PageIterator(Generic[_ItemTypeDef], PageIterator):
    def __iter__(self) -> Iterator[_ItemTypeDef]:
        """
        Proxy method to specify iterator item type.
        """

class DescribeTablePaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.DescribeTable)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#describetablepaginator)
    """

    def paginate(
        self,
        *,
        Database: str,
        ClusterIdentifier: str = ...,
        ConnectedDatabase: str = ...,
        DbUser: str = ...,
        Schema: str = ...,
        SecretArn: str = ...,
        Table: str = ...,
        WorkgroupName: str = ...,
        PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[DescribeTableResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.DescribeTable.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#describetablepaginator)
        """

class GetStatementResultPaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.GetStatementResult)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#getstatementresultpaginator)
    """

    def paginate(
        self, *, Id: str, PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[GetStatementResultResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.GetStatementResult.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#getstatementresultpaginator)
        """

class ListDatabasesPaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListDatabases)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listdatabasespaginator)
    """

    def paginate(
        self,
        *,
        Database: str,
        ClusterIdentifier: str = ...,
        DbUser: str = ...,
        SecretArn: str = ...,
        WorkgroupName: str = ...,
        PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[ListDatabasesResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListDatabases.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listdatabasespaginator)
        """

class ListSchemasPaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListSchemas)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listschemaspaginator)
    """

    def paginate(
        self,
        *,
        Database: str,
        ClusterIdentifier: str = ...,
        ConnectedDatabase: str = ...,
        DbUser: str = ...,
        SchemaPattern: str = ...,
        SecretArn: str = ...,
        WorkgroupName: str = ...,
        PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[ListSchemasResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListSchemas.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listschemaspaginator)
        """

class ListStatementsPaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListStatements)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#liststatementspaginator)
    """

    def paginate(
        self,
        *,
        RoleLevel: bool = ...,
        StatementName: str = ...,
        Status: StatusStringType = ...,
        PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[ListStatementsResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListStatements.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#liststatementspaginator)
        """

class ListTablesPaginator(Paginator):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListTables)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listtablespaginator)
    """

    def paginate(
        self,
        *,
        Database: str,
        ClusterIdentifier: str = ...,
        ConnectedDatabase: str = ...,
        DbUser: str = ...,
        SchemaPattern: str = ...,
        SecretArn: str = ...,
        TablePattern: str = ...,
        WorkgroupName: str = ...,
        PaginationConfig: PaginatorConfigTypeDef = ...
    ) -> _PageIterator[ListTablesResponseTypeDef]:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Paginator.ListTables.paginate)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_redshift_data/paginators/#listtablespaginator)
        """
