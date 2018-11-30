#!/usr/bin/env python3
import unittest
import requests
import requests_mock


class TestEndpoints(unittest.TestCase):
    """
    Contains functions that:
        1. Check every route on the website (including any general pages (home, about, etc.).
        2. Check every endpoint for each api.

    Tests are performend by creating mock data and requesting it in order to make assert checks for equality.
    """

    @requests_mock.mock()
    def test_general(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: general')

        home = 'https://www.abupower.com'
        about = 'https://www.abupower.com/about'

        m.get(home, text='1')
        m.get(about, text='2')

        self.assertEqual(requests.get(home).text, '1')
        self.assertEqual(requests.get(about).text, '2')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')

    @requests_mock.mock()
    def test_alpha(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: alpha')

        alphaPowerTable = 'https://www.abupower.com/api/alpha/power/table'
        alphaPowerActive = 'https://www.abupower.com/api/alpha/power/active'
        alphaPowerIndexAvg = 'https://www.abupower.com/api/alpha/power/index/avg'
        alphaPowerCheck1 = 'https://www.abupower.com/api/alpha/power/MOXRUBY'
        alphaPowerCheck2 = 'https://www.abupower.com/api/alpha/power/mox%20ruby'
        alphaDualsTable = 'https://www.abupower.com/api/alpha/duals/table'
        alphaDualsCheck1 = 'https://www.abupower.com/api/alpha/duals/underground-sea'
        alphaDualsCheck2 = 'https://www.abupower.com/api/alpha/duals/undergroundSea'
        alpha = 'https://www.abupower.com/alpha'


        m.get(alphaPowerTable, text='1')
        m.get(alphaPowerActive, text='2')
        m.get(alphaPowerIndexAvg, text='3')
        m.get(alphaPowerCheck1, text='4')
        m.get(alphaPowerCheck2, text='5')
        m.get(alphaDualsTable, text='6')
        m.get(alphaDualsCheck1, text='7')
        m.get(alphaDualsCheck2, text='8')
        m.get(alpha, text='9')

        self.assertEqual(requests.get(alphaPowerTable).text, '1')
        self.assertEqual(requests.get(alphaPowerActive).text, '2')
        self.assertEqual(requests.get(alphaPowerIndexAvg).text, '3')
        self.assertEqual(requests.get(alphaPowerCheck1).text, '4')
        self.assertEqual(requests.get(alphaPowerCheck2).text, '5')
        self.assertEqual(requests.get(alphaDualsTable).text, '6')
        self.assertEqual(requests.get(alphaDualsCheck1).text, '7')
        self.assertEqual(requests.get(alphaDualsCheck2).text, '8')
        self.assertEqual(requests.get(alpha).text, '9')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')

    @requests_mock.mock()
    def test_beta(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: beta')
        
        betaPowerTable = 'https://www.abupower.com/api/beta/power/table'
        betaPowerActive = 'https://www.abupower.com/api/beta/power/active'
        betaPowerIndexAvg = 'https://www.abupower.com/api/beta/power/index/avg'
        betaPowerCheck1 = 'https://www.abupower.com/api/beta/power/MOXRUBY'
        betaPowerCheck2 = 'https://www.abupower.com/api/beta/power/mox%20ruby'
        betaDualsTable = 'https://www.abupower.com/api/beta/duals/table'
        betaDualsCheck1 = 'https://www.abupower.com/api/beta/duals/underground-sea'
        betaDualsCheck2 = 'https://www.abupower.com/api/beta/duals/undergroundSea'
        beta = 'https://www.abupower.com/beta'

        m.get(betaPowerTable, text='1')
        m.get(betaPowerActive, text='2')
        m.get(betaPowerIndexAvg, text='3')
        m.get(betaPowerCheck1, text='4')
        m.get(betaPowerCheck2, text='5')
        m.get(betaDualsTable, text='6')
        m.get(betaDualsCheck1, text='7')
        m.get(betaDualsCheck2, text='8')
        m.get(beta, text='9')

        self.assertEqual(requests.get(betaPowerTable).text, '1')
        self.assertEqual(requests.get(betaPowerActive).text, '2')
        self.assertEqual(requests.get(betaPowerIndexAvg).text, '3')
        self.assertEqual(requests.get(betaPowerCheck1).text, '4')
        self.assertEqual(requests.get(betaPowerCheck2).text, '5')
        self.assertEqual(requests.get(betaDualsTable).text, '6')
        self.assertEqual(requests.get(betaDualsCheck1).text, '7')
        self.assertEqual(requests.get(betaDualsCheck2).text, '8')
        self.assertEqual(requests.get(beta).text, '9')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')

    @requests_mock.mock()
    def test_unlimited(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: unlimited')
        
        unlimitedPowerTable = 'https://www.abupower.com/api/unlimited/power/table'
        unlimitedPowerActive = 'https://www.abupower.com/api/unlimited/power/active'
        unlimitedPowerIndexAvg = 'https://www.abupower.com/api/unlimited/power/index/avg'
        unlimitedPowerCheck1 = 'https://www.abupower.com/api/unlimited/power/MOXRUBY'
        unlimitedPowerCheck2 = 'https://www.abupower.com/api/unlimited/power/mox%20ruby'
        unlimitedDualsTable = 'https://www.abupower.com/api/unlimited/duals/table'
        unlimitedDualsCheck1 = 'https://www.abupower.com/api/unlimited/duals/underground-sea'
        unlimitedDualsCheck2 = 'https://www.abupower.com/api/unlimited/duals/undergroundSea'
        unlimited = 'https://www.abupower.com/unlimited'

        m.get(unlimitedPowerTable, text='1')
        m.get(unlimitedPowerActive, text='2')
        m.get(unlimitedPowerIndexAvg, text='3')
        m.get(unlimitedPowerCheck1, text='4')
        m.get(unlimitedPowerCheck2, text='5')
        m.get(unlimitedDualsTable, text='6')
        m.get(unlimitedDualsCheck1, text='7')
        m.get(unlimitedDualsCheck2, text='8')
        m.get(unlimited, text='9')

        self.assertEqual(requests.get(unlimitedPowerTable).text, '1')
        self.assertEqual(requests.get(unlimitedPowerActive).text, '2')
        self.assertEqual(requests.get(unlimitedPowerIndexAvg).text, '3')
        self.assertEqual(requests.get(unlimitedPowerCheck1).text, '4')
        self.assertEqual(requests.get(unlimitedPowerCheck2).text, '5')
        self.assertEqual(requests.get(unlimitedDualsTable).text, '6')
        self.assertEqual(requests.get(unlimitedDualsCheck1).text, '7')
        self.assertEqual(requests.get(unlimitedDualsCheck2).text, '8')
        self.assertEqual(requests.get(unlimited).text, '9')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')

    @requests_mock.mock()
    def test_revised(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: revised')
        
        revisedDualsTable = 'https://www.abupower.com/api/revised/duals/table'
        revisedDualsCheck1 = 'https://www.abupower.com/api/revised/duals/underground-sea'
        revisedDualsCheck2 = 'https://www.abupower.com/api/revised/duals/undergroundSea'
        revised = 'https://www.abupower.com/revised'

        m.get(revisedDualsTable, text='1')
        m.get(revisedDualsCheck1, text='2')
        m.get(revisedDualsCheck2, text='3')
        m.get(revised, text='4')

        self.assertEqual(requests.get(revisedDualsTable).text, '1')
        self.assertEqual(requests.get(revisedDualsCheck1).text, '2')
        self.assertEqual(requests.get(revisedDualsCheck2).text, '3')
        self.assertEqual(requests.get(revised).text, '4')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')

    @requests_mock.mock()
    def test_ce(self, m):
        print('----------------------------------------------------------------------')
        print('Testing endpoints: ce')
        
        cePowerTable = 'https://www.abupower.com/api/ce/power/table'
        cePowerActive = 'https://www.abupower.com/api/ce/power/active'
        cePowerIndexAvg = 'https://www.abupower.com/api/ce/power/index/avg'
        cePowerCheck1 = 'https://www.abupower.com/api/ce/power/MOXRUBY'
        cePowerCheck2 = 'https://www.abupower.com/api/ce/power/mox%20ruby'
        ceDualsTable = 'https://www.abupower.com/api/ce/duals/table'
        ceDualsCheck1 = 'https://www.abupower.com/api/ce/duals/underground-sea'
        ceDualsCheck2 = 'https://www.abupower.com/api/ce/duals/undergroundSea'
        ce = 'https://www.abupower.com/collectors'

        m.get(cePowerTable, text='1')
        m.get(cePowerActive, text='2')
        m.get(cePowerIndexAvg, text='3')
        m.get(cePowerCheck1, text='4')
        m.get(cePowerCheck2, text='5')
        m.get(ceDualsTable, text='6')
        m.get(ceDualsCheck1, text='7')
        m.get(ceDualsCheck2, text='8')
        m.get(ce, text='9')

        self.assertEqual(requests.get(cePowerTable).text, '1')
        self.assertEqual(requests.get(cePowerActive).text, '2')
        self.assertEqual(requests.get(cePowerIndexAvg).text, '3')
        self.assertEqual(requests.get(cePowerCheck1).text, '4')
        self.assertEqual(requests.get(cePowerCheck2).text, '5')
        self.assertEqual(requests.get(ceDualsTable).text, '6')
        self.assertEqual(requests.get(ceDualsCheck1).text, '7')
        self.assertEqual(requests.get(ceDualsCheck2).text, '8')
        self.assertEqual(requests.get(ce).text, '9')

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')


def main():
    unittest.main()


if __name__ == '__main__':
      main()
