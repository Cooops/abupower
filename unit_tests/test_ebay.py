#!/usr/bin/env python3

import os
import unittest
import re
from ebaysdk.utils import dict2xml

os.environ.setdefault("EBAY_CONFIG", "CONFIG.ini")


class TestEbay(unittest.TestCase):

    def test_motors_compat_request_xml(self):
        print('----------------------------------------------------------------------')
        print('Testing ebay: general')

        motors_dict = {
            'Item': {
                'Category': '101',
                'Title': 'My Title',
                'ItemCompatibilityList': {
                    'Compatibility': [
                        {
                            'CompatibilityNotes': 'Fits for all trims and engines.',
                            'NameValueList': [
                                {'Name': 'Year', 'Value': '2001'},
                                {'Name': 'Make', 'Value': 'Honda'},
                                {'Name': 'Model', 'Value': 'Accord'}
                            ]
                        },
                    ]
                }
            }
        }

        motors_xml = """<Item>
                            <Category>101</Category>
                            <ItemCompatibilityList>
                                <Compatibility>
                                    <CompatibilityNotes>Fits for all trims and engines.</CompatibilityNotes>
                                    <NameValueList>
                                        <Name>Year</Name><Value>2001</Value>
                                    </NameValueList>
                                    <NameValueList>
                                        <Name>Make</Name><Value>Honda</Value>
                                    </NameValueList>
                                    <NameValueList>
                                        <Name>Model</Name><Value>Accord</Value>
                                    </NameValueList>
                                </Compatibility>
                            </ItemCompatibilityList>
                            <Title>My Title</Title>
                        </Item>"""

        motors_xml = re.sub(r'>\s+<', '><', motors_xml)
        motors_xml = re.sub(r'\s+$', '', motors_xml)

        self.assertEqual(dict2xml(motors_dict), motors_xml)

        print('All tests passed ✅')
        print('----------------------------------------------------------------------')


def main():
    unittest.main()


if __name__ == '__main__':
      main()
