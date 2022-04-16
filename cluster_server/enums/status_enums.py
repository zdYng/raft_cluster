from enum import Enum, unique


@unique
class NodeHealthStatusEnum(Enum):
    """
        节点状态枚举
    """

    # 健康状态
    __ERROR_HEALTH_STATUS__ = -1  # error 错误状态
    __OFFLINE_HEALTH_STATUS__ = 0  # offline 离线
    __ONLINE_HEALTH_STATUS__ = 1  # online 在线


@unique
class NodeWorkingStatusEnum(Enum):
    # 工作状态
    __ERROR_WORKING_STATUS__ = -1  # error 错误工作状态
    __IDLE_WORKING_STATUS__ = 0  # idle 空闲工作状态
    __BUSY_WORKING_STATUS__ = 1  # busy 繁忙工作状态


@unique
class NodeLoadAverageStatusEnum(Enum):
    # 负载状态
    __ERROR_LOAD_AVERAGE_STATUS__ = -1  # error 错误负载状态
    __ZERO_LOAD_AVERAGE_STATUS__ = 0  # 0 负载状态
    __LOW_LOAD_AVERAGE_STATUS__ = 1  # 低负载状态 < 50%
    __COMMON_LOAD_AVERAGE_STATUS__ = 2  # 50% < 正常负载状态 < 90% 服务器负载正常，
    __FULL_LOAD_AVERAGE_STATUS__ = 3  # 90% < 满负载 <= 100%

