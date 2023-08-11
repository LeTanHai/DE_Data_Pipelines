"""
Type annotations for rds service client waiters.

[Open documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/)

Usage::

    ```python
    from boto3.session import Session

    from mypy_boto3_rds.client import RDSClient
    from mypy_boto3_rds.waiter import (
        DBClusterAvailableWaiter,
        DBClusterDeletedWaiter,
        DBClusterSnapshotAvailableWaiter,
        DBClusterSnapshotDeletedWaiter,
        DBInstanceAvailableWaiter,
        DBInstanceDeletedWaiter,
        DBSnapshotAvailableWaiter,
        DBSnapshotCompletedWaiter,
        DBSnapshotDeletedWaiter,
    )

    session = Session()
    client: RDSClient = session.client("rds")

    db_cluster_available_waiter: DBClusterAvailableWaiter = client.get_waiter("db_cluster_available")
    db_cluster_deleted_waiter: DBClusterDeletedWaiter = client.get_waiter("db_cluster_deleted")
    db_cluster_snapshot_available_waiter: DBClusterSnapshotAvailableWaiter = client.get_waiter("db_cluster_snapshot_available")
    db_cluster_snapshot_deleted_waiter: DBClusterSnapshotDeletedWaiter = client.get_waiter("db_cluster_snapshot_deleted")
    db_instance_available_waiter: DBInstanceAvailableWaiter = client.get_waiter("db_instance_available")
    db_instance_deleted_waiter: DBInstanceDeletedWaiter = client.get_waiter("db_instance_deleted")
    db_snapshot_available_waiter: DBSnapshotAvailableWaiter = client.get_waiter("db_snapshot_available")
    db_snapshot_completed_waiter: DBSnapshotCompletedWaiter = client.get_waiter("db_snapshot_completed")
    db_snapshot_deleted_waiter: DBSnapshotDeletedWaiter = client.get_waiter("db_snapshot_deleted")
    ```
"""
from typing import Sequence

from botocore.waiter import Waiter

from .type_defs import FilterTypeDef, WaiterConfigTypeDef

__all__ = (
    "DBClusterAvailableWaiter",
    "DBClusterDeletedWaiter",
    "DBClusterSnapshotAvailableWaiter",
    "DBClusterSnapshotDeletedWaiter",
    "DBInstanceAvailableWaiter",
    "DBInstanceDeletedWaiter",
    "DBSnapshotAvailableWaiter",
    "DBSnapshotCompletedWaiter",
    "DBSnapshotDeletedWaiter",
)


class DBClusterAvailableWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterAvailable)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclusteravailablewaiter)
    """

    def wait(
        self,
        *,
        DBClusterIdentifier: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterAvailable.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclusteravailablewaiter)
        """


class DBClusterDeletedWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterDeleted)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclusterdeletedwaiter)
    """

    def wait(
        self,
        *,
        DBClusterIdentifier: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterDeleted.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclusterdeletedwaiter)
        """


class DBClusterSnapshotAvailableWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterSnapshotAvailable)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclustersnapshotavailablewaiter)
    """

    def wait(
        self,
        *,
        DBClusterIdentifier: str = ...,
        DBClusterSnapshotIdentifier: str = ...,
        SnapshotType: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        IncludePublic: bool = ...,
        DbClusterResourceId: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterSnapshotAvailable.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclustersnapshotavailablewaiter)
        """


class DBClusterSnapshotDeletedWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterSnapshotDeleted)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclustersnapshotdeletedwaiter)
    """

    def wait(
        self,
        *,
        DBClusterIdentifier: str = ...,
        DBClusterSnapshotIdentifier: str = ...,
        SnapshotType: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        IncludePublic: bool = ...,
        DbClusterResourceId: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBClusterSnapshotDeleted.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbclustersnapshotdeletedwaiter)
        """


class DBInstanceAvailableWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBInstanceAvailable)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbinstanceavailablewaiter)
    """

    def wait(
        self,
        *,
        DBInstanceIdentifier: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBInstanceAvailable.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbinstanceavailablewaiter)
        """


class DBInstanceDeletedWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBInstanceDeleted)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbinstancedeletedwaiter)
    """

    def wait(
        self,
        *,
        DBInstanceIdentifier: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBInstanceDeleted.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbinstancedeletedwaiter)
        """


class DBSnapshotAvailableWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotAvailable)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotavailablewaiter)
    """

    def wait(
        self,
        *,
        DBInstanceIdentifier: str = ...,
        DBSnapshotIdentifier: str = ...,
        SnapshotType: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        IncludePublic: bool = ...,
        DbiResourceId: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotAvailable.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotavailablewaiter)
        """


class DBSnapshotCompletedWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotCompleted)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotcompletedwaiter)
    """

    def wait(
        self,
        *,
        DBInstanceIdentifier: str = ...,
        DBSnapshotIdentifier: str = ...,
        SnapshotType: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        IncludePublic: bool = ...,
        DbiResourceId: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotCompleted.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotcompletedwaiter)
        """


class DBSnapshotDeletedWaiter(Waiter):
    """
    [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotDeleted)
    [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotdeletedwaiter)
    """

    def wait(
        self,
        *,
        DBInstanceIdentifier: str = ...,
        DBSnapshotIdentifier: str = ...,
        SnapshotType: str = ...,
        Filters: Sequence[FilterTypeDef] = ...,
        MaxRecords: int = ...,
        Marker: str = ...,
        IncludeShared: bool = ...,
        IncludePublic: bool = ...,
        DbiResourceId: str = ...,
        WaiterConfig: WaiterConfigTypeDef = ...
    ) -> None:
        """
        [Show boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Waiter.DBSnapshotDeleted.wait)
        [Show boto3-stubs documentation](https://youtype.github.io/boto3_stubs_docs/mypy_boto3_rds/waiters/#dbsnapshotdeletedwaiter)
        """
