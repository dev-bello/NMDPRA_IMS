# NMDPRA Inventory Management System

A comprehensive inventory management system built with Flask for the Nigerian Midstream and Downstream Petroleum Regulatory Authority (NMDPRA).

## Features

### Core Features
- Role-based access control (Admin and Staff roles)
- Microsoft Outlook integration for authentication
- Product management with image support
- Order request and checkout system
- Staff support and communication pipeline
- Notification system for low stock and requests
- Admin dashboard with reporting capabilities

### Admin Features
- Full CRUD operations
- User management
- IT support integration via WhatsApp
- Order request approval/rejection system
- Weekly and monthly PDF reports
- Staff management
- Order history tracking
- Company/Supplier management

### Staff Features
- Inventory viewing
- Order request submission
- Order tracking
- Support request system

## Tech Stack
- Backend: Python Flask
- Database: MySQL
- Authentication: Microsoft Outlook OAuth
- Frontend: HTML, CSS, JavaScript
- Additional: Flask-SQLAlchemy, Flask-Login, Flask-WTF

## Project Structure
```
nmdpra_ims/
├── app/
│   ├── __init__.py
│   ├── models/
│   ├── routes/
│   ├── static/
│   ├── templates/
│   └── utils/
├── config.py
├── requirements.txt
├── run.py
└── .env
```

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/yourusername/nmdpra_ims.git
cd nmdpra_ims
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

5. Initialize the database:
```bash
flask db init
flask db migrate
flask db upgrade
```

6. Run the application:
```bash
python run.py
```

## Development Guidelines

### Code Style
- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings to all functions and classes
- Write unit tests for new features

### Git Workflow
1. Create feature branch from develop
2. Make changes and commit with meaningful messages
3. Push changes and create pull request
4. Code review required before merging

### Database Migrations
- Always create migrations when changing models
- Test migrations before deploying
- Keep migrations in version control

## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the LICENSE file for details. # NMDPRA_IMS
