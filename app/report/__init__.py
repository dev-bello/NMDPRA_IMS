from flask import Blueprint

reports = Blueprint('reports', __name__, url_prefix='/admin/reports')

from . import views 