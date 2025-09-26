#!/usr/bin/env python3
"""
Google Sheets Credentials Setup Helper
Based on the original service.py credential approach
"""

import os
import json
import subprocess

def print_env_setup():
    """Print instructions for setting up environment variables"""
    print("🔧 GOOGLE SHEETS CREDENTIALS SETUP")
    print("=" * 60)

    print("You need to set these environment variables:")
    print()
    print("export GOOGLE_CLIENT_ID='your_client_id_here'")
    print("export GOOGLE_CLIENT_SECRET='your_client_secret_here'")
    print("export GOOGLE_ACCESS_TOKEN='your_access_token_here'")
    print("export GOOGLE_REFRESH_TOKEN='your_refresh_token_here'")
    print("export GOOGLE_SCOPE='https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive'")
    print()

    print("💡 HOW TO GET THESE VALUES:")
    print()
    print("1. 🌐 **Get Client ID & Secret:**")
    print("   - Go to: https://console.cloud.google.com/")
    print("   - Create/select project")
    print("   - Enable Google Sheets API")
    print("   - Create OAuth 2.0 credentials")
    print("   - Download credentials.json")
    print()

    print("2. 🔑 **Get Access & Refresh Tokens:**")
    print("   - Use OAuth 2.0 Playground: https://developers.google.com/oauthplayground/")
    print("   - Select Google Sheets API v4 scopes")
    print("   - Authorize and exchange authorization code")
    print("   - Copy the access_token and refresh_token")
    print()

    print("3. 📋 **Alternative - From existing credentials:**")
    print("   - If you have token.json or similar, extract the values")
    print("   - Look for 'token', 'refresh_token', 'client_id', 'client_secret'")
    print()

def check_env_vars():
    """Check if environment variables are set"""
    required_vars = [
        'GOOGLE_CLIENT_ID',
        'GOOGLE_CLIENT_SECRET',
        'GOOGLE_ACCESS_TOKEN',
        'GOOGLE_REFRESH_TOKEN'
    ]

    missing = []
    present = []

    for var in required_vars:
        if os.getenv(var):
            present.append(var)
        else:
            missing.append(var)

    print("🔍 ENVIRONMENT VARIABLES CHECK")
    print("=" * 60)

    if present:
        print("✅ Present:")
        for var in present:
            value = os.getenv(var)
            print(f"   {var}: {value[:20]}..." if len(value) > 20 else f"   {var}: {value}")

    if missing:
        print("\n❌ Missing:")
        for var in missing:
            print(f"   {var}")

    print()
    return len(missing) == 0

def extract_from_credentials_json():
    """Try to extract info from credentials.json if it exists"""
    creds_file = 'credentials.json'
    if os.path.exists(creds_file):
        print("📁 Found credentials.json")
        try:
            with open(creds_file, 'r') as f:
                creds = json.load(f)

            if 'installed' in creds:
                client_info = creds['installed']
                print(f"   Client ID: {client_info.get('client_id', 'Not found')}")
                print(f"   Client Secret: {client_info.get('client_secret', 'Not found')}")
                print("   ⚠️  Still need access_token and refresh_token from OAuth flow")

        except Exception as e:
            print(f"   ❌ Error reading file: {e}")
    else:
        print("📁 No credentials.json found")

def extract_from_token_json():
    """Try to extract info from token.json if it exists"""
    token_file = 'token.json'
    if os.path.exists(token_file):
        print("📁 Found token.json")
        try:
            with open(token_file, 'r') as f:
                token = json.load(f)

            print(f"   Access Token: {token.get('token', 'Not found')[:20]}...")
            print(f"   Refresh Token: {token.get('refresh_token', 'Not found')[:20]}...")
            print(f"   Client ID: {token.get('client_id', 'Not found')}")
            print(f"   Client Secret: {token.get('client_secret', 'Not found')[:20]}...")

            if all(key in token for key in ['token', 'refresh_token', 'client_id', 'client_secret']):
                print("\n💡 You can set environment variables from this file:")
                print(f"export GOOGLE_ACCESS_TOKEN='{token['token']}'")
                print(f"export GOOGLE_REFRESH_TOKEN='{token['refresh_token']}'")
                print(f"export GOOGLE_CLIENT_ID='{token['client_id']}'")
                print(f"export GOOGLE_CLIENT_SECRET='{token['client_secret']}'")

        except Exception as e:
            print(f"   ❌ Error reading file: {e}")
    else:
        print("📁 No token.json found")

def main():
    print("🚀 Google Sheets Credentials Setup Helper")
    print("Based on original service.py approach")
    print("=" * 60)

    # Check current environment
    if check_env_vars():
        print("🎉 All required environment variables are set!")
        print("You can now run:")
        print("   python tests/test_real_google_sheets_env.py")
        return

    print()

    # Try to extract from existing files
    extract_from_credentials_json()
    print()
    extract_from_token_json()
    print()

    # Print setup instructions
    print_env_setup()

    print("🧪 AFTER SETTING UP:")
    print("Run the test with:")
    print("   python tests/test_real_google_sheets_env.py")
    print()
    print("This will test the REAL Google Sheets:")
    print(f"   📗 Read-Write: https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit")
    print(f"   📘 Read-Only: https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit")

if __name__ == "__main__":
    main()