import calendar
from datetime import datetime, timedelta
import io
import logging
import os
import pandas as pd
import requests
import sys

def setupLogger(logDir=None, logFile=None,
                logFormat=logging.Formatter('%(asctime)s :: %(funcName)s :: %(lineno)d ... %(message)s'),
                logLevel=logging.INFO):
    logger = logging.getLogger('')
    logger.handlers = [] # Clears ancestor handlers
    logger.setLevel(logLevel)

    if logDir and logFile:
        fh = logging.FileHandler(os.path.join(logDir, logFile))
        fh.setFormatter(logFormat)
        logger.addHandler(fh)

        if screenLogging == 'y':
            sh = logging.StreamHandler(sys.stdout)
            sh.setFormatter(logFormat)
            logger.addHandler(sh)

        return logger

    else:
        sys.exit('Check logging directory and log output file name.')

def cldyCall(token, start, end, dimensions, metrics, sort, filters=None):
    base = f'https://app.cloudability.com/api/1/reporting/cost/run.csv?auth_token={token}'
    start_date = f'start_date={start}'
    end_date = f'end_date={end}'
    dimensions = 'dimensions='+','.join(dimensions)
    metrics = 'metrics='+','.join(metrics)
    sort = f'sort_by={sort}'
    order = f'order=asc'
    if filters == None:
        url = base+'&'+start_date+'&'+end_date+'&'+dimensions+'&'+metrics+'&'+sort+'&'+order
    else:
        filters = 'filters='+','.join(filters)
        url = base+'&'+start_date+'&'+end_date+'&'+dimensions+'&'+metrics+'&'+sort+'&'+order+'&'+filters

    try:
        df = pd.read_csv(url)
    except urllib.error.HTTPError:
        sys.exit('Invalid API request.')
    else:
        df = pd.read_csv(url)
        if df.columns[0][2:7]=='error':
            sys.exit('Invalid API response.')

    return df

def splitOrg(df, account, start, end):
    # Gets list of accounts under product master payer
    dimensions = [usageAccount]
    metrics = [cost]
    sort = usageAccount
    filters = ['account_identifier=@6191']
    split = cldyCall(token, start, end, dimensions, metrics, sort, filters)

    accts = list(split[usageAccount])

    product = pd.DataFrame(data=df[df[account].isin(accts)])
    platform = pd.DataFrame(data=df[~df[account].isin(accts)])

    return product, platform

def dfSquash(data, groupCol, metric):
    # Creates dataframe from groupby object
    group = data.groupby(groupCol)
    agg_fun = {metric: ['sum']}
    df = group.agg(agg_fun)
    df.reset_index(inplace=True)
    df.columns = df.columns.get_level_values(0)

    return df

def enterpriseSupport():
    # Checks for enterprise support fee
    log.info('Calling Cloudability API for support fee.')

    dimensions = [payerAccount, usageAccount]
    metrics = [cost]
    sort = payerAccount
    filters = ['item_description==AWS+Support+%28Enterprise%29']
    supCharges = cldyCall(token, start, end, dimensions, metrics, sort, filters)

    if supCharges[cost].sum() > 0:
        isEntSup = 1
    else:
        isEntSup = 0

    log.info('Calling Cloudability API for total supported charges by account.')

    dimensions = [usageAccount]
    metrics = [cost]
    sort = usageAccount
    filters = [usageAccount+'=='+x for x in entSupAccts]
    filters.append('item_description!=AWS+Support+%28Enterprise%29')
    supportedCost = cldyCall(token, start, end, dimensions, metrics, sort, filters)

    supportedCost[costCenter] = supportedCost[usageAccount].map(payerParent)
    actual = supportedCost[cost].sum()

    if isEntSup == 1:
        total = supCharges[cost].sum()
    else:
        log.info('No support fee found - generating estimate.')

        # https://aws.amazon.com/premiumsupport/pricing/
        if actual >= 1000000:
            subTotal = (actual-1000000)*0.03
            total = (subTotal+64500)*0.8
        elif 500000 <= actual < 1000000:
            subTotal = (totalSpend-500000)*0.05
            total = (subTotal+39500)*0.8
        elif 150000 <= actual < 500000:
            subTotal = (actual-150000)*0.07
            total = (subTotal+15000)*0.8
        else:
            total = (actual*0.1)*0.8

    log.info('Calculating support fee allocation by account.')

    supportedCost.loc[:,cost] = (supportedCost[cost] / actual)*total
    supportedCost = supportedCost[[usageAccount, costCenter, cost]]

    return supportedCost

def riPurchases():
    # Checks for ri purchase fees
    log.info('Calling Cloudability API for RI purchases.')

    dimensions = [payerAccount, 'tag2']
    metrics = [cost]
    sort = payerAccount
    filters = ['item_description=@ri%20cancellation',
               'item_description=@sign%20up%20charge%20for%20subscription']
    prePay = cldyCall(token, start, end, dimensions, metrics, sort, filters)

    if prePay.empty:
        log.info('No reserved instance purchases found - moving on.')
    else:
        log.info('Allocating reserved instance purchases.')

        prePay.loc[:,costCenter] = 'Pre-pay'
        prePay.loc[:,cost] *= 0.9 # Accounts for our edp discount

    return prePay

def allocation():
    # Allocates all usage, support, and ri fees
    log.info('Calling Cloudability API for usage data.')

    dimensions = [usageAccount, 'tag9', 'tag2']
    metrics = [cost]
    sort = usageAccount
    filters = ['item_description!=@ri%20cancellation',
               'item_description!=@sign%20up%20charge%20for%20subscription',
               'item_description!=AWS+Support+%28Enterprise%29']
    aws = cldyCall(token, start, end, dimensions, metrics, sort, filters)

    log.info('Cleansing cost center tag data.')

    # Clusters don't follow our tagging schema, and staging clusters
    # paid for by cost center other than account owner
    prdCluster = ~aws['cluster'].str.contains('stg')
    aws[costCenter].where(cond=prdCluster, other='220', inplace=True)
    aws.drop(['cluster'], axis=1, inplace=True)

    valCc = aws[costCenter].isin(['100', '220', '910', '(not set)'])
    aws[costCenter].where(cond=valCc, other='(not set)', inplace=True)

    isSet = aws[costCenter] != '(not set)'
    parentCc = aws[usageAccount].map(payerParent)
    aws[costCenter].where(cond=isSet, other=parentCc, inplace=True)

    if fullMonth == False:
        log.info('Adjusting accrual estimate for full month.')

        # Recurring fees are front loaded, artificially inflate
        # accrual estimates if not considered separately
        awsSum = aws[cost].sum()

        firstDay = cldyCall(token, start, start, dimensions, metrics, sort, filters)
        firstSum = firstDay[cost].sum()

        restSum = ((awsSum - firstSum)/(curMonthLimit-1))*(fullDays-1)

        aws[cost] = aws[cost].apply(lambda x: ((x/awsSum)*restSum)+((x/awsSum)*firstSum))

    log.info('Calling enterprise support function.')

    support = enterpriseSupport()

    log.info('Calling reserved instance purchases function.')

    ris = riPurchases()

    localInvoice = invoice

    if localInvoice == 0:
        log.info('Concatenating datasets.')

        awsTotal = pd.concat([aws, support, ris], ignore_index=True, sort=True)
    else:
        log.info('Concatenating datasets and adjusting for invoice total.')

        # Adjust allocation to match invoice total
        # Also see comment under if __name__ == __main__
        awsEs = pd.concat([aws, support], ignore_index=True, sort=True)
        localInvoice -= ris[cost].sum()
        awsEs.loc[:,'invoiceEstimate'] = (awsEs[cost]/awsEs[cost].sum())*localInvoice
        awsEs.loc[:,cost] = awsEs.loc[:,'invoiceEstimate']
        awsTotal = pd.concat([awsEs, ris], ignore_index=True, sort=True)

    log.info('Creating final dataframe with GL strings in place of cost centers.')

    invAlloc = dfSquash(awsTotal, costCenter, cost)

    invAlloc[cost] = round(invAlloc[cost], 2)
    invAlloc[costCenter] = invAlloc[costCenter].map(glDict)

    invAlloc.sort_values(by=costCenter, axis=0, inplace=True)
    invAlloc.reset_index(inplace=True, drop=True)

    return invAlloc, support, ris

def allocResults():
    # Combines all results for delivery

    log.info('Calling allocation function.')

    invAlloc, supportFees, riFees = allocation()

    if round(invAlloc[cost].sum(),2) != float(invoice) and float(invoice) != 0:
        sys.exit('Invoice amount and allocation total unequal. Please contact an admin.')

    # Break out support fees separately
    supportSquash = dfSquash(supportFees, costCenter, cost)
    supportSquash[cost] = round(supportSquash[cost],2)

    if not riFees.empty:
        # Map ri fees to pre-pay gl
        riFees[costCenter] = riFees[payerAccount].map(payerParent)
        # Break out ri fees separately
        risSquash = dfSquash(riFees, costCenter, cost)
        risSquash[cost] = round(risSquash[cost],2)

        return invAlloc, supportSquash, risSquash
    else:
        return invAlloc, supportSquash

if __name__ == '__main__':
    # Set up logging variables
    loggingDir = '/Users/christian.woodruff/Documents/Code/Scripts/AWS/Logging/'
    today = datetime.now()
    stringToday = today.strftime("%Y%m%d_%H%M%S")
    loggingFile = f'awsAllocator_{stringToday}.log'

    # If user select no, logger still returns file logging
    screenLogging = input('Print logs to screen? (y/n): ')
    if not screenLogging in ['y', 'Y', 'yes', 'Yes']:
        print('Screen logging disabled. Check logging directory for log files.')
        screenLogging = 'n'
    else:
        screenLogging = 'y'

    log = setupLogger(loggingDir, loggingFile)


    log.info('Setting up program variables.')

    # Accounts that use premium support
    entSupAccts = [
        # Account numbers hidden
    ]

    # Internal gl strings
    glDict = {str(x): '6795-'+str(x)+'-00' for x in range(101,1000)}
    glDict['100'] = '5605-000-00'
    glDict['Pre-pay'] = '1310-000-00'

    # Cost centers that pay for unallocated account charges
    payerParent = {
        # Account numbers hidden
    }

    # Cost centers that are responsible for unallocated account charges
    ownerParent = {
        # Account numbers hidden
    }

    # Attributes for Cloudability API calls
    cost = 'unblended_cost'
    payerAccount = 'account_identifier'
    usageAccount = 'vendor_account_identifier'
    costCenter = 'cost_center'

    token = input('Enter Cloudability API token: ')

    if len(token) != 20:
        sys.exit('Invalid Cloudability API token. Please try again.')

    invoice = 0

    curMonthFirst = today.replace(day=1)
    prevMonthLast = curMonthFirst - timedelta(days=1)

    curMonthName = calendar.month_name[curMonthFirst.month]
    prevMonthName = calendar.month_name[prevMonthLast.month]

    # AWS data can take up to 48 hours to fully appear
    curMonthLimit = today.day-2

    # Before the 20th not enough data exists to make an accurate monthly estimate
    reliableDay = 20

    log.info('Presenting user options.')

    if curMonthLimit >= reliableDay:
        option = input(f'''\nPlease choose from the following:\n
            1    ALLOCATION of {prevMonthName}'s invoice
            2    ACCRUAL ESTIMATE for {prevMonthName}
            3    ACCRUAL ESTIMATE for {curMonthName}\n\n''')
    else:
        options = [1, 2] # Checked later to ensure users don't enter 3
        option = input(f'''\nPlease choose from the following:\n
            1    ALLOCATION of {prevMonthName}'s invoice
            2    ACCRUAL ESTIMATE for {prevMonthName}\n
            --Accrual estimates for {curMonthName} unavailable until the {reliableDay}th--\n\n''')

    # sys.exit message
    invalidOption = 'Invalid option. Please try again.'

    try:
        int(option)
    except ValueError:
        sys.exit(invalidOption)
    else:
        if int(option) in (1,2):
            if int(option) == 1:
                # Allocation sum can differ from invoice amount due to rounding errors
                # Invoice amount used to mold allocation to match
                invoice = input('\nEnter invoice total as float, or 0 to estimate\n\n')
                try:
                    float(invoice)
                except ValueError:
                    sys.exit('Invalid invoice amount. Please try again.')
                else:
                    # Total spend well over 250k - warn about errant user input
                    if float(invoice) < 250000 and float(invoice) != 0:
                        print('\nWARNING: Invoice total entered is lower than usual.')
                    invoice = float(invoice)
            start = f'{prevMonthLast.year}-{prevMonthLast.strftime("%m")}-01'
            end = f'{start[:8]}{prevMonthLast.day}'
            fullMonth = True
        elif int(option) == 3:
            if not int(option) in options:
                sys.exit(invalidOption)
            start = f'{curMonthFirst.year}-{curMonthFirst.strftime("%m")}-01'
            end = f'{start[:8]}{curMonthLimit}'
            fullMonth = False
            fullDays = calendar.monthrange(curMonthFirst.year, curMonthFirst.month)[1]
        else:
            sys.exit(invalidOption)

    log.info('Gathering results.')

    resultsCount = len(allocResults())
    if resultsCount == 2:
        invAlloc, supportSquash = allocResults()
    else:
        invAlloc, supportSquash, risSquash = allocResults()

    log.info('Printing results.')

    print('\n\nAllocation:')
    print(f'{invAlloc.to_string(index=False, header=False)}')
    print('\n')
    print('Enterprise Support Breakout:')
    print(f'{supportSquash.to_string(index=False, header=False)}')
    print('\n')
    try:
        risSquash
    except NameError:
        pass
    else:
        print('New Pre-pay Breakout:')
        print(f'{risSquash.to_string(index=False, header=False)}')

#     __Sample output__

#         Allocation:
#         1310-000-00   29374.20
#         5605-000-00  139270.85
#         6795-220-00  101823.59
#         6795-910-00  173809.11


#         Enterprise Support Breakout:
#         100  8738.86
#         220  3361.14
#         910  7156.41


#         New Pre-pay Breakout:
#         220  20340.9
#         910   9033.3
