# **Getting Started**

## **Prerequisites**

### **Setting Up Your Machine**
1. **Python3**: Ensure you have Python3 installed. You can download and install it from the [official Python website](https://www.python.org/downloads/).
   
2. **Pipenv**: After installing Python3, you'll need to install Pipenv. This can be done via pip:
    ```bash
    pip install pipenv
    ```

## **Registering Your Key on a Platform**

1. Open your terminal or command prompt.

2. Navigate to the `utils` directory:
    ```bash
    cd path/to/utils
    ```

3. Use the `autheid_upload_key.sh` script to generate and register a new key. The script requires the following parameters:
   - Environment (e.g., `dev`)
   - API key name (e.g., `api_key_1`)
   - Account email (e.g., `my.account@mail.com`)

    Here's how you run it:

    ```bash
    ./autheid_upload_key.sh [environment] [api_key_name] [account_email]
    ```

    **Example**:
    ```bash
    ./autheid_upload_key.sh dev api_key_1 my.account@mail.com
    ```

    Running the above command will:
   - Generate and register a new key for the account `my.account@mail.com` in the `dev` environment.
   - Store the generated key in the `dev/api_key_1/key.pem` file.
