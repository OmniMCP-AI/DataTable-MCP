# Google Sheets API Setup Guide

## 🚀 Quick Setup for Real Google Sheets Testing

To run the real Google Sheets verification test, you need to set up Google Sheets API credentials.

### 📋 Step-by-Step Setup:

#### 1. **Go to Google Cloud Console**
- Visit: https://console.cloud.google.com/
- Sign in with your Google account

#### 2. **Create or Select Project**
- Create a new project OR select an existing one
- Note the project name for reference

#### 3. **Enable Google Sheets API**
- Go to "APIs & Services" > "Library"
- Search for "Google Sheets API"
- Click on it and press "ENABLE"

#### 4. **Create Credentials**
- Go to "APIs & Services" > "Credentials"
- Click "CREATE CREDENTIALS" > "OAuth 2.0 Client IDs"
- Choose "Desktop application" as application type
- Give it a name (e.g., "DataTable MCP Test")

#### 5. **Download Credentials**
- After creating, click the download icon (⬇️)
- Save the JSON file as `credentials.json` in the project root directory
- **Important**: Place it in `/Users/dengwei/work/ai/om3/DataTable-MCP/credentials.json`

#### 6. **Run the Test**
```bash
cd /Users/dengwei/work/ai/om3/DataTable-MCP
python tests/test_real_google_sheets_verification.py
```

### 🔐 Authentication Flow:

1. **First Run**: Browser will open for OAuth consent
2. **Sign In**: Use your Google account
3. **Grant Permissions**: Allow access to Google Sheets
4. **Token Saved**: `token.json` will be created automatically
5. **Future Runs**: Will use saved token (no browser needed)

### 📊 Test Spreadsheets:

The test will verify permissions on these **REAL** spreadsheets:

- **📗 Read-Write**: `https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit`
- **📘 Read-Only**: `https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit`
- **❌ Invalid**: `invalid_sheet_id_that_does_not_exist`

### 🧪 What the Test Does:

1. **Permission Verification**:
   - Checks read/write permissions on each spreadsheet
   - Verifies expected vs actual permissions

2. **Real Operations**:
   - **Read Test**: Reads actual data from the spreadsheets
   - **Write Test**: Attempts to write data (succeeds on read-write, fails on read-only)

3. **Results**:
   - ✅ Shows which operations work
   - ❌ Shows permission errors
   - 🌐 Provides clickable URLs to verify manually

### 🎯 Expected Results:

```
✅ PASS Read-Write Sheet
  Expected: READ_WRITE
  Actual: READ_WRITE
  🌐 URL: https://docs.google.com/spreadsheets/d/1p5Yjvqw-jv6MHClvplqsod5NcoF9-mm4zaYutt-i95M/edit

✅ PASS Read-Only Sheet
  Expected: READ_ONLY
  Actual: READ_ONLY
  🌐 URL: https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit

✅ PASS Invalid Sheet
  Expected: NOT_EXISTS
  Actual: NOT_EXISTS
```

### 🛠️ Troubleshooting:

**❌ "credentials.json not found"**
- Make sure the file is in the project root directory
- Check the filename is exactly `credentials.json`

**❌ "Authentication failed"**
- Check your internet connection
- Make sure Google Sheets API is enabled in your project
- Try deleting `token.json` and re-authenticating

**❌ "Permission denied"**
- Make sure you have access to the test spreadsheets
- Check if your Google account has the necessary permissions

### 🎉 Success Output:

After successful setup and run, you'll see:
- Real permission verification results
- Actual read/write test results on the spreadsheets
- Clickable URLs to manually verify the data

This gives you a **real use case** with **actual Google Sheets** instead of mock data!