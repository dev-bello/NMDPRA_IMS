from flask import Blueprint

inventory = Blueprint('inventory', __name__, url_prefix='/inventory')

from . import views