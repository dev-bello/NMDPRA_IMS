from flask import Blueprint

purchases = Blueprint('purchases', __name__, url_prefix='/purchases')

from . import views