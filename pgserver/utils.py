#!/usr/bin/env/python
import weakref

class WeakMethodPartial(object):
    def __init__(self, instance, method_name, *args, **kwargs):
        self.referenced = True
        self.instance_ref = weakref.ref(instance, self._dereferenced)

        # These are not weak references, but we do release them
        # if the instance is deleted.
        self.method_name = method_name
        self.args = args
        self.kwargs = kwargs

    def _dereferenced(self, also_self):
        self.referenced = False

        del self.method_name
        del self.args
        del self.kwargs

    def __call__(self, *args, **kwargs):
        if not self.referenced:
            return

        instance = self.instance_ref()

        cur_args = self.args + args

        cur_kwargs = dict(self.kwargs)
        cur_kwargs.update(args)

        return getattr(instance, self.method_name)(*cur_args, **cur_kwargs)
