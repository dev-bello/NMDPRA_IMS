from flask import Blueprint, request, render_template, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from app.models.request import Request, RequestItem, RequestStatus, ItemRequestStatus, DirectorateEnum
from app.models.inventory import Inventory  
from app import db
from datetime import datetime, UTC
from app.request import request as request_bp
from app.models.request import RequestStatus

def get_stock_status(quantity):
    """Helper function to determine stock status based on quantity."""
    if quantity == 0:
        return 'out-of-stock'
    elif quantity < 15:
        return 'low-stock'
    else:
        return 'in-stock'

def get_stock_status_text(quantity):
    """Helper function to get stock status text."""
    if quantity == 0:
        return 'Out of Stock'
    elif quantity < 15:
        return 'Low Stock'
    else:
        return 'In Stock'


@request_bp.route('/create', methods=['GET', 'POST'])
@login_required
def create_request():
    """Create a new request."""
    directorate_choices = [d.value for d in DirectorateEnum]
    request_form = {}

    if request.method == 'POST':
        try:
            data = request.form
            location = data.get('location')
            directorate = data.get('directorate')
            department = data.get('department')
            unit = data.get('unit')
            # TODO: Remove the request date functionality.
            # The following line that retrieves the 'request_date' from the form should be removed.
            # START: MODIFICATION - Get request date from form
            request_date_str = data.get('request_date')
            # END: MODIFICATION
            items = []
            
            # Process form data for items
            inventory_ids = request.form.getlist('inventory_id')
            quantities = request.form.getlist('quantity')
            
            # Create items list from form data
            for i in range(len(inventory_ids)):
                if inventory_ids[i] and quantities[i]:
                    items.append({
                        'inventory_id': inventory_ids[i],
                        'quantity': quantities[i]
                    })

            # Save form values for re-rendering in case of error
            request_form = {
                'directorate': directorate,
                'department': department,
                'unit': unit,
                'location': location,
                'inventory_ids': inventory_ids,
                'quantities': quantities,
                # TODO: Remove the request date functionality.
                # The 'request_date' key-value pair should be removed from this dictionary.
                'request_date': request_date_str
            }

            # Validation
            errors = []
            if not directorate:
                errors.append('Directorate is required.')
            if not unit:
                errors.append('Unit is required.')
            if not location or not items:
                errors.append('Location and items are required')

            if errors:
                for error in errors:
                    flash(error, 'error')
                inventories = Inventory.get_all_inventory()
                categories = {item.category.id: item.category for item in inventories}.values()
                return render_template('request/user/create.html', 
                                       directorate_choices=directorate_choices,
                                       request_form=request_form,
                                       inventories=inventories,
                                       categories=categories,
                                       get_stock_status=get_stock_status,
                                       get_stock_status_text=get_stock_status_text)
    

            # TODO: Remove the request date functionality.
            # The `request_date` argument should be removed from the `create_request` method call.
            # The method call should be updated to:
            # new_request, error = Request.create_request(
            #     user_id=current_user.id,
            #     location=location,
            #     directorate=directorate,
            #     department=department,
            #     unit=unit
            # )
            # Create the request
            new_request, error = Request.create_request(
                user_id=current_user.id,
                location=location,
                directorate=directorate,
                department=department,
                unit=unit,
                # START: MODIFICATION - Pass request date to model
                # The selected date is passed to the create_request method.
                request_date=datetime.strptime(request_date_str, '%Y-%m-%d') if request_date_str else None
                # END: MODIFICATION
            )
            if error:
                flash(error, 'error')
                inventories = Inventory.get_all_inventory()
                categories = {item.category.id: item.category for item in inventories}.values()
                return render_template('request/user/create.html', 
                                       directorate_choices=directorate_choices,
                                       request_form=request_form,
                                       inventories=inventories,
                                       categories=categories,
                                       get_stock_status=get_stock_status,
                                       get_stock_status_text=get_stock_status_text)
                

            # Add items to the request
            for item in items:
                inventory_id = item.get('inventory_id')
                quantity = item.get('quantity')

                if not inventory_id or not quantity:
                    db.session.rollback()
                    flash('Inventory ID and quantity are required for each item', 'error')
                    inventories = Inventory.get_all_inventory()
                    categories = {item.category.id: item.category for item in inventories}.values()
                    return render_template('request/user/create.html', 
                                           directorate_choices=directorate_choices,
                                           request_form=request_form,
                                           inventories=inventories,
                                           categories=categories,
                                           get_stock_status=get_stock_status,
                                           get_stock_status_text=get_stock_status_text)

                # --- START: Quantity validation ---
                inventory = Inventory.query.get(int(inventory_id))
                if not inventory:
                    db.session.rollback()
                    flash('Selected inventory item does not exist.', 'error')
                    inventories = Inventory.get_all_inventory()
                    categories = {item.category.id: item.category for item in inventories}.values()
                    return render_template('request/user/create.html', 
                                           directorate_choices=directorate_choices,
                                           request_form=request_form,
                                           inventories=inventories,
                                           categories=categories,
                                           get_stock_status=get_stock_status,
                                           get_stock_status_text=get_stock_status_text)

                if int(quantity) > inventory.quantity:
                    db.session.rollback()
                    flash(f"Requested quantity for {inventory.item_name} exceeds available stock ({inventory.quantity}).", "error")
                    inventories = Inventory.get_all_inventory()
                    categories = {item.category.id: item.category for item in inventories}.values()
                    return render_template('request/user/create.html', 
                                           directorate_choices=directorate_choices,
                                           request_form=request_form,
                                           inventories=inventories,
                                           categories=categories,
                                           get_stock_status=get_stock_status,
                                           get_stock_status_text=get_stock_status_text)
                # --- END: Quantity validation ---

                request_item, error = RequestItem.create_request_item(new_request.id, inventory_id, quantity)
                if error:
                    db.session.rollback()
                    flash(error, 'error')
                    inventories = Inventory.get_all_inventory()
                    categories = {item.category.id: item.category for item in inventories}.values()
                    return render_template('request/user/create.html', 
                                           directorate_choices=directorate_choices,
                                           request_form=request_form,
                                           inventories=inventories,
                                           categories=categories,
                                           get_stock_status=get_stock_status,
                                           get_stock_status_text=get_stock_status_text)

            db.session.commit()
            flash('Request created successfully', 'success')
            return redirect(url_for('request.get_my_requests'))

        except Exception as e:
            db.session.rollback()
            # current_app.logger.error(f"Error creating request: {str(e)}")
            flash('An error occurred while creating the request', 'error')
            inventories = Inventory.get_all_inventory()
            categories = {item.category.id: item.category for item in inventories}.values()
            return render_template('request/user/create.html', 
                                   directorate_choices=directorate_choices,
                                   request_form=request_form,
                                   inventories=inventories,
                                   categories=categories,
                                   get_stock_status=get_stock_status,
                                   get_stock_status_text=get_stock_status_text)

    
    # GET request - display the form
    inventories = Inventory.get_all_inventory()  # Fetch all inventory items
    categories = {item.category.id: item.category for item in inventories}.values()
    if not inventories:
        flash('No inventory items available', 'error')
        return render_template('request/user/create.html', 
                               directorate_choices=directorate_choices,
                               request_form=request_form,
                               inventories=inventories,
                               categories=categories,
                               get_stock_status=get_stock_status,
                               get_stock_status_text=get_stock_status_text)
    return render_template('request/user/create.html', 
                           directorate_choices=directorate_choices,
                           request_form=request_form,
                           inventories=inventories,
                           categories=categories,
                           get_stock_status=get_stock_status,
                           get_stock_status_text=get_stock_status_text)

@request_bp.route('/my-requests', methods=['GET'])
@login_required
def get_my_requests():
    """Get all requests for the current user."""
    try:
        # START: MODIFICATION - Handle search parameters
        # Get search parameters from the URL query string.
        search_item = request.args.get('search_item')
        search_date = request.args.get('search_date')
        # Pass search parameters to the model method.
        requests = Request.get_user_requests(current_user.id, search_item=search_item, search_date=search_date)
        # Pass search parameters to the template to pre-fill the search form.
        return render_template('request/user/list.html', requests=requests, search_item=search_item, search_date=search_date)
        # END: MODIFICATION
    except Exception as e:
        current_app.logger.error(f"Error fetching user requests: {str(e)}")
        flash('An error occurred while fetching your requests', 'error')
        return render_template('request/user/list.html', requests=[])

@request_bp.route('/all', methods=['GET'])
@login_required
def get_all_requests():
    """Get all requests (admin only)."""
    if not current_user.is_admin:
        flash('You do not have permission to access this page', 'error')
        return redirect(url_for('request.get_my_requests'))
    
    try:
        # START: MODIFICATION - Handle search parameters
        # Get search parameters from the URL query string.
        search_item = request.args.get('search_item')
        search_date = request.args.get('search_date')
        # Pass search parameters to the model method.
        requests = Request.get_all_requests(search_item=search_item, search_date=search_date)
        # Pass search parameters to the template to pre-fill the search form.
        return render_template('request/admin/list.html', requests=requests, search_item=search_item, search_date=search_date)
        # END: MODIFICATION
    except Exception as e:
        current_app.logger.error(f"Error fetching all requests: {str(e)}")
        flash('An error occurred while fetching requests', 'error')
        return render_template('request/admin/list.html', requests=[])

@request_bp.route('/<int:request_id>', methods=['GET'])
@login_required
def get_request(request_id):
    """Get a specific request by ID."""
    try:
        req = Request.get_request_by_id(request_id)
        if not req:
            flash('Request not found', 'error')
            return redirect(url_for('request.get_my_requests'))
        
        # Check if user is authorized to view this request
        if not current_user.is_admin and req.user_id != current_user.id:
            flash('You do not have permission to view this request', 'error')
            return redirect(url_for('request.get_my_requests'))

        # Choose template based on user role
        if current_user.is_admin:
            return render_template('request/admin/detail.html', request=req)
        else:
            return render_template('request/user/detail.html', request=req)
    except Exception as e:
        # current_app.logger.error(f"Error fetching request: {str(e)}")
        flash('An error occurred while fetching the request', 'error')
        if current_user.is_admin:
            return redirect(url_for('request.get_all_requests'))
        else:
            return redirect(url_for('request.get_my_requests'))

@request_bp.route('/<int:request_id>/status', methods=['GET', 'POST'])
@login_required
def update_request_status(request_id):
    """Update request status and item statuses (admin only)."""
    if not current_user.is_admin:
        flash('You do not have permission to update request status', 'error')
        return redirect(url_for('request.get_my_requests'))

    req = Request.get_request_by_id(request_id)
    if not req:
        flash('Request not found', 'error')
        return redirect(url_for('request.get_all_requests'))
    
    if request.method == 'GET':
        return render_template('request/admin/update_status.html', request=req)

    try:
        data = request.form
        admin_message = data.get('admin_message')

        # Update per-item status and approved quantity
        all_statuses = []
        for item in req.items:
            item_status = data.get(f'item_status_{item.id}')
            approved_quantity = data.get(f'approved_quantity_{item.id}')

            if item_status:
                if item_status == ItemRequestStatus.APPROVED.value:
                    if not item.validate_inventory_quantity(approved_quantity):
                        flash(f'Insufficient inventory for {item.inventory.item_name}', 'error')
                        return render_template('request/admin/update_status.html', request=req)
                    item.approve(int(approved_quantity))
                    all_statuses.append(ItemRequestStatus.APPROVED)
                elif item_status == ItemRequestStatus.REJECTED.value:
                    item.reject()
                    all_statuses.append(ItemRequestStatus.REJECTED)
                else:
                    all_statuses.append(ItemRequestStatus.PENDING)
            else:
                all_statuses.append(ItemRequestStatus.PENDING)
            item.updated_at = datetime.now(UTC)

        # Only update request status if all items have a non-pending status
        if all(s != ItemRequestStatus.PENDING for s in all_statuses):
            if all(s == ItemRequestStatus.APPROVED for s in all_statuses):
                req.status = RequestStatus.APPROVED
                req.approved_by = current_user.id  # Set approver
            elif all(s == ItemRequestStatus.REJECTED for s in all_statuses):
                req.status = RequestStatus.REJECTED
                req.approved_by = None  # No approver for rejected
            elif (ItemRequestStatus.APPROVED in all_statuses and 
                  ItemRequestStatus.REJECTED in all_statuses):
                req.status = RequestStatus.PARTIALLY_APPROVED
                req.approved_by = current_user.id  # Set approver for partial
            req.updated_at = datetime.now(UTC)
            if admin_message:
                req.admin_message = admin_message
            db.session.commit()
            flash('Request and items updated successfully', 'success')
            return redirect(url_for('request.get_request', request_id=request_id))
        else:
            if admin_message:
                req.admin_message = admin_message
            db.session.commit()
            flash('All items must have a status selected before updating the request status.', 'error')
            return render_template('request/admin/update_status.html', request=req)
    except Exception as e:
        db.session.rollback()
        # current_app.logger.error(f"Error updating request and items: {str(e)}")
        flash('An error occurred while updating the request', 'error')
        return render_template('request/admin/update_status.html', request=req)
    
    
@request_bp.route('/<int:request_id>/collect', methods=['GET', 'POST'])
@login_required
def mark_collected(request_id):
    """Mark request as collected (admin only)."""
    if not current_user.is_admin:
        flash('You do not have permission to mark requests as collected', 'error')
        return redirect(url_for('request.get_my_requests'))

    req = Request.get_request_by_id(request_id)
    if not req:
        flash('Request not found', 'error')
        return redirect(url_for('request.get_all_requests'))
    
    # GET request - show the collection form
    if request.method == 'GET':
        return render_template('request/admin/collect.html', request=req)
    
    # POST request - process the collection
    try:
        admin_note = request.form.get('admin_note')

        success, error = req.mark_collected(admin_note)
        if not success:
            flash(error, 'error')
            return render_template('request/admin/collect.html', request=req)

        flash('Request marked as collected successfully', 'success')
        return redirect(url_for('request.get_request', request_id=request_id))
    except Exception as e:
        db.session.rollback()
        # current_app.logger.error(f"Error marking request as collected: {str(e)}")
        flash('An error occurred while marking the request as collected', 'error')
        return render_template('request/admin/collect.html', request=req)

@request_bp.route('/<int:request_id>/delete', methods=['POST'])
@login_required
def delete_request(request_id):
    """Soft delete a request (admin only)."""
    
    req = Request.get_request_by_id(request_id)
    if not req:
        flash('Request not found', 'error')
        return redirect(url_for('request.get_all_requests'))
    if current_user.is_admin:
        if req.status == RequestStatus.COLLECTED:
            flash('Cannot delete a collected request', 'error')
            return redirect(url_for('request.get_request', request_id=request_id))

    elif req.user_id == current_user.id and req.status == RequestStatus.PENDING:
        pass

    else:
        flash('You do not have permission to delete this request', 'error')
        return redirect(url_for('request.get_my_requests'))
    
    try:
        reason = request.form.get('reason')
        success, error = req.soft_delete(current_user.id, reason)
        
        if not success:
            flash(error, 'error')
            return redirect(url_for('request.get_request', request_id=request_id))

        flash('Request deleted successfully', 'success')
        if current_user.is_admin:
            return redirect(url_for('request.get_all_requests'))
        else:   
            return redirect(url_for('request.get_my_requests'))
    except Exception as e:
        # current_app.logger.error(f"Error deleting request: {str(e)}")
        flash('An error occurred while deleting the request', 'error')
        return redirect(url_for('request.get_request', request_id=request_id))

@request_bp.route('/<int:request_id>/restore', methods=['POST'])
@login_required
def restore_request(request_id):
    """Restore a soft-deleted request (admin only)."""
    if not current_user.is_admin:
        flash('You do not have permission to restore requests', 'error')
        return redirect(url_for('request.get_my_requests'))

    req = Request.query.get(request_id)
    if not req:
        flash('Request not found', 'error')
        return redirect(url_for('request.get_all_requests'))
    
    try:
        success, error = req.restore()
        
        if not success:
            flash(error, 'error')
            return redirect(url_for('request.get_request', request_id=request_id))

        flash('Request restored successfully', 'success')
        return redirect(url_for('request.get_all_requests'))
    except Exception as e:
        # current_app.logger.error(f"Error restoring request: {str(e)}")
        flash('An error occurred while restoring the request', 'error')
        return redirect(url_for('request.get_request', request_id=request_id))
    
@request_bp.route('/<int:request_id>/permanent-delete', methods=['POST'])
@login_required
def permanent_delete_request(request_id):
    """Permanently delete a soft-deleted request (admin only)."""
    if not current_user.is_admin:
        flash('You do not have permission to permanently delete requests', 'error')
        return redirect(url_for('request.get_my_requests'))

    req = Request.query.get(request_id)
    if not req:
        flash('Request not found', 'error')
        return redirect(url_for('request.get_all_requests'))

    success, error = req.permanent_delete_if_soft_deleted()
    if success:
        flash('Request permanently deleted.', 'success')
    else:
        flash(error, 'error')
    return redirect(url_for('request.get_all_requests'))

@request_bp.route('/deleted', methods=['GET'])
@login_required
def get_deleted_requests():
    """Admin only: View all soft-deleted requests."""
    if not current_user.is_admin:
        flash('You do not have permission to access this page', 'error')
        return redirect(url_for('request.get_my_requests'))

    try:
        requests = Request.get_deleted_requests()
        return render_template('request/admin/deleted.html', requests=requests)
    except Exception as e:
        # current_app.logger.error(f"Error fetching deleted requests: {str(e)}")
        flash('An error occurred while fetching deleted requests', 'error')
        return render_template('request/admin/deleted.html', requests=[])
    
@request_bp.route('/deleted/delete-all', methods=['POST'])
@login_required
def delete_all_deleted_requests():
    """Admin only: Permanently delete all soft-deleted requests."""
    if not current_user.is_admin:
        flash('You do not have permission to perform this action', 'error')
        return redirect(url_for('request.get_my_requests'))

    try:
        deleted_requests = Request.get_deleted_requests()
        count = 0
        for req in deleted_requests:
            success, _ = req.permanent_delete_if_soft_deleted()
            if success:
                count += 1
        flash(f'{count} deleted request(s) permanently removed.', 'success')
    except Exception as e:
        # current_app.logger.error(f"Error deleting all deleted requests: {str(e)}")
        flash('An error occurred while deleting all deleted requests', 'error')
    return redirect(url_for('request.get_deleted_requests'))
