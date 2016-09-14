import abc
from six import with_metaclass
from collections import namedtuple
from intervaltree import IntervalTree, Interval
from itertools import groupby


Restriction = namedtuple(
    'Restriction', ['sid',
                    'effective_date',
                    'expiry_date',
                    'restriction_type']
)


class RestrictionsController(object):
    """
    Combines restriction information of various sources from multiple
    restriction managers
    """

    def __init__(self):
        self.rl_managers = []

    def add_restrictions(self, rl_manager):
        self.rl_managers.append(rl_manager)

    def restrictions(self, sid, dt):
        restrictions = [rlm.restrictions(sid, dt) for rlm in self.rl_managers]
        return set.union(*restrictions) if restrictions else {}

    def is_restricted(self, sid, dt):
        return len(self.restrictions(sid, dt)) != 0


class RLManager(with_metaclass(abc.ABCMeta)):
    """
    ABC for a restricted list manager that returns information about
    restrictions from a single source
    """

    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def restrictions(self, sid, dt):
        raise NotImplementedError

    @abc.abstractmethod
    def is_restricted(self, sid, dt):
        raise NotImplementedError


class StaticRestrictedList(RLManager):
    """
    Tracks a static list of sids that are restricted regardless of dt
    """

    def __init__(self, restricted_list):
        super(StaticRestrictedList, self).__init__()

        self._restricted_set = set(restricted_list)

    def restrictions(self, sid, dt):
        """
        A sid has a restriction of 'freeze' for all dts if it is in the static
        list
        """
        return {'freeze'} if self.is_restricted(sid, dt) else {}

    def is_restricted(self, sid, dt):
        """
        A sid is restricted for all dts if it is in the static list
        """
        return sid in self._restricted_set


class InMemoryRLManager(RLManager):
    """
    Tracks restrictions with defined start, end and type for different sids
    """

    def __init__(self, restrictions):
        super(InMemoryRLManager, self).__init__()

        # A dict mapping each sid to an IntervalTree of its restriction history
        self._restriction_intervals = {
            sid: IntervalTree(
                [Interval(rstn.effective_date,
                          rstn.expiry_date,
                          rstn.restriction_type)
                 for rstn in restrictions_for_sid])
            for sid, restrictions_for_sid
            in groupby(restrictions, lambda x: x.sid)
        }

    def restrictions(self, sid, dt):
        """
        Returns the restrictions for a sid on a dt
        """
        try:
            restrictions_for_sid = self._restriction_intervals[sid]
            return {rstn.data for rstn in restrictions_for_sid[dt]}
        except KeyError:
            return set()

    def is_restricted(self, sid, dt):
        """
        Returns whether or not a sid is restricted on a dt
        """
        return len(self.restrictions(sid, dt)) != 0
