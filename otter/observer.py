"""
Observer
"""

class Observer(MutiService, object):

    def __init__(self, dispatcher, kz_client, interval):
        super(Observer, self).__init__()
        self.lock = self.kz_client.Lock(path)
        timer = TimerService(interval, self._check_feeds)
        timer.setServiceParent(self)

    def stopService(self):
        d = super(Observer, self).stopService()
        return d.addCallback(lambda _: self.lock.release())

    def _check_feeds(self):
        if not self.lock.acquire(False, None):
            return
        return perform(self.disp, check_feeds())


@do
def check_feeds():
    last_marker = yield get_last_marker()
    entries, marker = yield read_entries(last_marker)
    groups = find_changed_groups(entries)
    trigger_convergence(groups)
    update_last_marker(marker)


def find_changed_groups(entries):
    """
    :param list entries: List of entry dicts from CF
    """
    return filter(None, map(find_changed_group, entries))


def find_changed_group(entry):
    payload = entry["content"]["@text"]["payload"]
    group_id = group_id_from_metadata(payload["metdata"])
    return (payload["tenant_id"], group_id) if group_id is not None else None



