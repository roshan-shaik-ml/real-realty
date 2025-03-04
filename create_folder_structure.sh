#!/bin/bash

# Ensure the script is run inside the existing project folder
if [ ! -f "README.md" ]; then
    echo "Error: Run this script inside your project folder!"
    exit 1
fi

# Create necessary directories
mkdir -p app/{api/routes,core,db/{models,schemas,repositories},services,crawler,utils}
mkdir -p tests scripts

# Create required files
touch app/{main.py,__init__.py}
touch app/api/{__init__.py,dependencies.py}
touch app/api/routes/{houses.py,buyers.py,sellers.py,__init__.py}
touch app/core/{config.py,__init__.py}
touch app/db/{mongodb.py,postgresql.py,__init__.py}
touch app/db/models/{house.py,buyer.py,seller.py,__init__.py}
touch app/db/schemas/{house.py,buyer.py,seller.py,__init__.py}
touch app/db/repositories/{house_repo.py,buyer_repo.py,seller_repo.py,__init__.py}
touch app/services/{lead_service.py,__init__.py}
touch app/crawler/{zillow_scraper.py,scheduler.py,__init__.py}
touch app/utils/{logger.py,__init__.py}

# Confirm structure creation
echo "FastAPI project structure created successfully!"
