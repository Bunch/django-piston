import warnings

from utils import rc
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned, FieldError
from django.db.models import ForeignKey, Model
from django.conf import settings

typemapper = { }
handler_tracker = [ ]

class HandlerMetaClass(type):
    """
    Metaclass that keeps a registry of class -> handler
    mappings.
    """
    def __new__(cls, name, bases, attrs):
        new_cls = type.__new__(cls, name, bases, attrs)

        def already_registered(model, anon):
            for k, (m, a, d) in typemapper.iteritems():
                if model == m and anon == a:
                    return (k, d)

            return (None, False)

        if hasattr(new_cls, 'model'):
            (old_cls, default) = already_registered(new_cls.model, new_cls.is_anonymous)

            if old_cls and default == new_cls.default_for_model:
                if not getattr(settings, 'PISTON_IGNORE_DUPE_MODELS', False):
                    warnings.warn("Handler already registered for model %s, "
                        "you may experience inconsistent results." % new_cls.model.__name__)

            if not old_cls or new_cls.default_for_model >= default:
                if old_cls:
                    del typemapper[old_cls]
                typemapper[new_cls] = (new_cls.model, new_cls.is_anonymous, new_cls.default_for_model)
        else:
            typemapper[new_cls] = (None, new_cls.is_anonymous, new_cls.default_for_model)

        if name not in ('BaseHandler', 'AnonymousBaseHandler'):
            handler_tracker.append(new_cls)

        return new_cls

class BaseHandler(object):
    """
    Basehandler that gives you CRUD for free.
    You are supposed to subclass this for specific
    functionality.

    All CRUD methods (`read`/`update`/`create`/`delete`)
    receive a request as the first argument from the
    resource. Use this for checking `request.user`, etc.
    """
    __metaclass__ = HandlerMetaClass

    allowed_methods = ('GET', 'POST', 'PUT', 'DELETE')
    anonymous = is_anonymous = False
    exclude = ( 'id', )
    fields =  ( )
    default_for_model = False

    def flatten_dict(self, dct):
        return dict([ (str(k), dct.get(k)) for k in dct.keys() ])

    def has_model(self):
        return hasattr(self, 'model') or hasattr(self, 'queryset')

    def queryset(self, request):
        return self.model.objects.all()

    def value_from_tuple(tu, name):
        for int_, n in tu:
            if n == name:
                return int_
        return None

    def exists(self, **kwargs):
        if not self.has_model():
            raise NotImplementedError

        try:
            self.model.objects.get(**kwargs)
            return True
        except self.model.DoesNotExist:
            return False

    def read(self, request, *args, **kwargs):
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        pkfield = self.model._meta.pk.name

        # Rename foreign keys to the __pk syntax for filters
        for f in self.model._meta.fields:
            if isinstance(f, ForeignKey) and kwargs.has_key(f.name):
                # Ensure we don't already have a model instance
                if not isinstance(kwargs[f.name], Model):
                    kwargs[f.name + '__pk'] = kwargs.pop(f.name)

        if pkfield in kwargs:
            try:
                return self.queryset(request).get(pk=kwargs.get(pkfield))
            except ObjectDoesNotExist:
                return rc.NOT_FOUND
            except MultipleObjectsReturned: # should never happen, since we're using a PK
                return rc.BAD_REQUEST
        else:
            return self.queryset(request).filter(*args, **kwargs)

    def create(self, request, *args, **kwargs):
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        # Use keyword arguments to override
        # data specified in request
        attrs = self.flatten_dict(request.data)
        attrs.update(kwargs)

        # Separate instance values and
        # foreign key values
        ids = {}

        # Rename foreign keys to the _id syntax for assignment
        for f in self.model._meta.fields:
            if isinstance(f, ForeignKey) and attrs.has_key(f.name):
                # Ensure we don't already have a model instance
                if not isinstance(attrs[f.name], Model):
                    ids[f.name + '_id'] = attrs.pop(f.name)

        try:
            inst = self.queryset(request).get(**attrs)
            return rc.DUPLICATE_ENTRY
        except self.model.DoesNotExist:
            inst = self.model(**attrs)

            # Assign IDs for foreign keys
            for (k, v) in ids.items():
                setattr(inst, k, v)

            inst.save()
            return inst
        except self.model.MultipleObjectsReturned:
            return rc.DUPLICATE_ENTRY
        except FieldError:
            return rc.BAD_REQUEST

    def update(self, request, *args, **kwargs):
        if not self.has_model():
            return rc.NOT_IMPLEMENTED

        pkfield = self.model._meta.pk.name
        attrs = self.flatten_dict(request.data)

        if pkfield not in kwargs or not attrs:
            # No pk was specified
            return rc.BAD_REQUEST

        try:
            inst = self.queryset(request).get(pk=kwargs.get(pkfield))
        except ObjectDoesNotExist:
            return rc.NOT_FOUND
        except MultipleObjectsReturned: # should never happen, since we're using a PK
            return rc.BAD_REQUEST

        for k,v in attrs.iteritems():
            setattr( inst, k, v )

        inst.save()
        return rc.ALL_OK

    def delete(self, request, *args, **kwargs):
        if not self.has_model():
            raise NotImplementedError

        try:
            inst = self.queryset(request).get(*args, **kwargs)

            inst.delete()

            return rc.DELETED
        except self.model.MultipleObjectsReturned:
            return rc.DUPLICATE_ENTRY
        except self.model.DoesNotExist:
            return rc.NOT_HERE

class AnonymousBaseHandler(BaseHandler):
    """
    Anonymous handler.
    """
    is_anonymous = True
    allowed_methods = ('GET',)
