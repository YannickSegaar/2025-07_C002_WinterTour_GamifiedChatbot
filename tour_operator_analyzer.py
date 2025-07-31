import asyncio
import re
import pandas as pd
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TourOperatorAnalyzer:
    def __init__(self):
        # Default parameters
        self.delay = 1
        self.timeout = 60000
        self.max_pages_per_site = 5  # Reduced since we need to analyze content
        self.concurrency = 20  # Reduced for more thorough analysis
        
        # Detection patterns
        self.chatbot_patterns = {
            # JavaScript libraries and widgets
            'intercom': ['intercom', 'widget.intercom.io'],
            'zendesk': ['zendesk', 'zopim', 'zdchat'],
            'drift': ['drift.com', 'js.driftt.com'],
            'tidio': ['tidio', 'code.tidio.co'],
            'tawk': ['tawk.to', 'embed.tawk.to'],
            'livechat': ['livechatinc', 'cdn.livechatinc.com'],
            'crisp': ['crisp.chat', 'client.crisp.chat'],
            'hubspot': ['hubspot', 'js.hs-analytics.net'],
            'freshchat': ['freshchat', 'wchat.freshchat.com'],
            'olark': ['olark.com', 'static.olark.com'],
            'smartsupp': ['smartsupp.com'],
            'chatlio': ['chatlio.com'],
            'pure_chat': ['purechat.com'],
            'chat_widget': ['chat-widget', 'chatwidget', 'chat_widget'],
            'messenger': ['messenger', 'connect.facebook.net']
        }
        
        self.booking_patterns = {
            'fareharbor': ['fareharbor.com', 'fareharbor', 'fh-button', 'fh-widget'],
            'resd': ['resd.com', 'res-d.com', 'resd-booking'],
            'woocommerce': ['woocommerce', 'wc-', 'wp-content/plugins/woocommerce'],
            'shopify': ['shopify', 'cdn.shopify.com', 'shop.app'],
            'bookeo': ['bookeo.com', 'bookeo'],
            'checkfront': ['checkfront.com', 'checkfront'],
            'peek': ['peek.com', 'bookwithpeek'],
            'rezdy': ['rezdy.com', 'rezdy'],
            'regiondo': ['regiondo.com', 'regiondo'],
            'trekksoft': ['trekksoft.com', 'trekksoft'],
            'bokun': ['bokun.io', 'bokun.com'],
            'viator': ['viator.com', 'viator'],
            'stripe': ['stripe.com', 'js.stripe.com'],
            'square': ['squareup.com', 'square'],
            'paypal': ['paypal.com', 'paypal'],
            'book_now': ['book-now', 'booknow', 'reservation'],
            'calendar_booking': ['calendly.com', 'acuityscheduling']
        }
        
        self.ota_patterns = {
            'getyourguide': ['getyourguide.com', 'getyourguide'],
            'viator': ['viator.com', 'tripadvisor'],
            'tripadvisor': ['tripadvisor.com', 'tripadvisor'],
            'expedia': ['expedia.com', 'expedia'],
            'airbnb': ['airbnb.com', 'airbnb'],
            'booking': ['booking.com', 'booking'],
            'klook': ['klook.com', 'klook'],
            'tiqets': ['tiqets.com', 'tiqets'],
            'headout': ['headout.com', 'headout'],
            'musement': ['musement.com', 'musement'],
            'citypass': ['citypass.com', 'citypass'],
            'gocity': ['gocity.com', 'smartdestinations'],
            'isango': ['isango.com', 'isango'],
            'attractiontix': ['attractiontix.com'],
            'veltra': ['veltra.com', 'veltra']
        }

    def analyze_page_content(self, html_content, page_text, page_url):
        """Analyze page content for chatbots, booking tech, and OTA dependencies."""
        html_lower = html_content.lower()
        text_lower = page_text.lower()
        combined_content = html_lower + " " + text_lower
        
        results = {
            'has_chatbot': False,
            'chatbot_types': [],
            'booking_technology': [],
            'ota_dependencies': [],
            'analysis_details': {}
        }
        
        # 1. KNOWN PATTERN DETECTION (Original method)
        # Detect known chatbots
        for chatbot_type, patterns in self.chatbot_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    results['has_chatbot'] = True
                    if chatbot_type not in results['chatbot_types']:
                        results['chatbot_types'].append(chatbot_type)
                    break
        
        # 2. BEHAVIORAL CHATBOT DETECTION (New intelligent method)
        if not results['has_chatbot']:
            chatbot_indicators = self._detect_unknown_chatbot(html_content, page_text)
            if chatbot_indicators['likely_chatbot']:
                results['has_chatbot'] = True
                results['chatbot_types'].extend(chatbot_indicators['evidence'])
        
        # 3. KNOWN BOOKING TECHNOLOGY DETECTION
        for booking_type, patterns in self.booking_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    if booking_type not in results['booking_technology']:
                        results['booking_technology'].append(booking_type)
                    break
        
        # 4. BEHAVIORAL BOOKING DETECTION (New intelligent method)
        unknown_booking = self._detect_unknown_booking_system(html_content, page_text)
        results['booking_technology'].extend(unknown_booking)
        
        # 5. KNOWN OTA DETECTION
        for ota_type, patterns in self.ota_patterns.items():
            for pattern in patterns:
                if pattern in combined_content:
                    if ota_type not in results['ota_dependencies']:
                        results['ota_dependencies'].append(ota_type)
                    break
        
        # 6. BEHAVIORAL OTA DETECTION (New intelligent method)
        unknown_ota = self._detect_unknown_ota_integration(html_content, page_text, page_url)
        results['ota_dependencies'].extend(unknown_ota)
        
        # Enhanced analysis
        results['analysis_details'] = {
            'has_online_booking': any(keyword in combined_content for keyword in 
                                    ['book online', 'book now', 'reserve now', 'buy tickets', 'purchase']),
            'has_contact_form': any(keyword in combined_content for keyword in 
                                  ['contact form', 'contact us', 'get in touch', 'enquiry']),
            'mentions_commission': any(keyword in combined_content for keyword in 
                                     ['commission', 'booking fee', 'service fee']),
            'has_live_chat_ui': self._has_chat_ui_elements(html_content),
            'has_booking_widgets': self._has_booking_widgets(html_content),
            'external_booking_links': self._count_external_booking_links(html_content, page_url)
        }
        
        return results

    def _detect_unknown_chatbot(self, html_content, page_text):
        """Detect chatbots using behavioral analysis instead of known patterns."""
        evidence = []
        score = 0
        
        # Look for chat-related UI elements
        chat_ui_indicators = [
            # CSS classes that suggest chat functionality
            r'class="[^"]*chat[^"]*"', r'class="[^"]*message[^"]*"',
            r'class="[^"]*widget[^"]*"', r'class="[^"]*bubble[^"]*"',
            r'id="[^"]*chat[^"]*"', r'id="[^"]*message[^"]*"',
            
            # Chat-specific HTML structures
            r'<div[^>]*chat[^>]*>', r'<iframe[^>]*chat[^>]*>',
            r'data-[^=]*chat[^=]*=', r'aria-label="[^"]*chat[^"]*"',
            
            # JavaScript chat functions
            r'function[^{]*chat[^{]*{', r'\.chat\s*\(',
            r'chat\s*:', r'chatbot', r'livechat'
        ]
        
        for pattern in chat_ui_indicators:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                score += len(matches)
                evidence.append(f"chat_ui_elements ({len(matches)} found)")
        
        # Look for chat-specific text content
        chat_text_indicators = [
            'start chat', 'chat with us', 'live chat', 'chat now',
            'send message', 'type your message', 'chat support',
            'online support', 'ask us anything', 'need help?',
            'how can we help', 'chat bubble', 'minimize chat'
        ]
        
        for indicator in chat_text_indicators:
            if indicator in page_text.lower():
                score += 2
                evidence.append(f"chat_text: '{indicator}'")
        
        # Look for chat-related JavaScript events
        js_chat_indicators = [
            'onclick.*chat', 'onload.*chat', 'chat.*function',
            'websocket', 'socket.io', 'chat.*api'
        ]
        
        for pattern in js_chat_indicators:
            if re.search(pattern, html_content, re.IGNORECASE):
                score += 3
                evidence.append(f"js_chat_function")
        
        # Check for floating action buttons (common for chat)
        fab_patterns = [
            r'position:\s*fixed[^}]*bottom[^}]*right',
            r'position:\s*fixed[^}]*right[^}]*bottom',
            r'class="[^"]*float[^"]*"[^>]*chat',
            r'style="[^"]*z-index:\s*999[^"]*"'
        ]
        
        for pattern in fab_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                score += 2
                evidence.append("floating_chat_button")
        
        return {
            'likely_chatbot': score >= 5,  # Threshold for detection
            'confidence_score': score,
            'evidence': evidence if score >= 5 else []
        }

    def _detect_unknown_booking_system(self, html_content, page_text):
        """Detect booking systems using behavioral analysis."""
        booking_systems = []
        
        # Look for booking form structures
        booking_form_patterns = [
            r'<form[^>]*book[^>]*>', r'<form[^>]*reserv[^>]*>',
            r'<form[^>]*ticket[^>]*>', r'input[^>]*date[^>]*',
            r'select[^>]*guest[^>]*>', r'select[^>]*person[^>]*>',
            r'input[^>]*quantity[^>]*>', r'button[^>]*book[^>]*>'
        ]
        
        form_matches = sum(1 for pattern in booking_form_patterns 
                          if re.search(pattern, html_content, re.IGNORECASE))
        
        if form_matches >= 3:
            booking_systems.append('custom_booking_form')
        
        # Look for calendar/date picker widgets
        calendar_patterns = [
            'datepicker', 'calendar-widget', 'date-selector',
            'flatpickr', 'pikaday', 'datejs', 'moment.js'
        ]
        
        if any(pattern in html_content.lower() for pattern in calendar_patterns):
            booking_systems.append('calendar_booking_widget')
        
        # Look for payment integration
        payment_patterns = [
            'payment-form', 'credit-card', 'card-number',
            'billing-address', 'cvv', 'expiry'
        ]
        
        if any(pattern in html_content.lower() for pattern in payment_patterns):
            booking_systems.append('integrated_payment_system')
        
        # Check for booking-related JavaScript APIs
        api_patterns = [
            r'api[/.]book', r'booking[/.]api', r'/reservation',
            r'ajax.*book', r'xhr.*reserv'
        ]
        
        if any(re.search(pattern, html_content, re.IGNORECASE) for pattern in api_patterns):
            booking_systems.append('custom_booking_api')
        
        return booking_systems

    def _detect_unknown_ota_integration(self, html_content, page_text, page_url):
        """Detect OTA dependencies using behavioral analysis."""
        ota_integrations = []
        
        try:
            domain = urlparse(page_url).netloc
            
            # Look for external booking redirects
            external_booking_patterns = [
                r'href="https?://(?!' + re.escape(domain) + r').*book',
                r'href="https?://(?!' + re.escape(domain) + r').*reserv',
                r'href="https?://(?!' + re.escape(domain) + r').*ticket',
            ]
            
            external_links = []
            for pattern in external_booking_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                external_links.extend(matches)
            
            if len(external_links) >= 2:
                ota_integrations.append('external_booking_redirects')
        except Exception:
            pass
        
        # Look for OTA-style pricing displays
        pricing_patterns = [
            'from.*per person', 'starting from', 'price from',
            'adult.*child.*price', 'group discount'
        ]
        
        pricing_matches = sum(1 for pattern in pricing_patterns 
                            if re.search(pattern, page_text, re.IGNORECASE))
        
        if pricing_matches >= 2:
            ota_integrations.append('ota_style_pricing')
        
        # Look for availability checking without direct booking
        availability_patterns = [
            'check availability', 'select date.*check',
            'availability calendar', 'real.*time.*availability'
        ]
        
        has_availability = any(re.search(pattern, page_text, re.IGNORECASE) 
                             for pattern in availability_patterns)
        
        # If they have availability checking but no direct booking form, likely OTA dependent
        has_direct_booking = any(pattern in html_content.lower() 
                               for pattern in ['<form', 'book now', 'add to cart'])
        
        if has_availability and not has_direct_booking:
            ota_integrations.append('availability_only_no_direct_booking')
        
        return ota_integrations

    def _has_chat_ui_elements(self, html_content):
        """Check for chat UI elements in the HTML."""
        chat_ui_selectors = [
            'chat-widget', 'chat-bubble', 'chat-button',
            'message-input', 'chat-container', 'live-chat'
        ]
        return any(selector in html_content.lower() for selector in chat_ui_selectors)

    def _has_booking_widgets(self, html_content):
        """Check for booking widget elements."""
        booking_selectors = [
            'booking-widget', 'reservation-form', 'book-now',
            'date-picker', 'guest-selector', 'booking-calendar'
        ]
        return any(selector in html_content.lower() for selector in booking_selectors)

    def _count_external_booking_links(self, html_content, page_url):
        """Count links that redirect to external booking platforms."""
        try:
            domain = urlparse(page_url).netloc
            # Find all external links with booking-related terms
            link_pattern = r'href="(https?://(?!' + re.escape(domain) + r')[^"]*(?:book|reserv|ticket|buy)[^"]*)"'
            external_booking_links = re.findall(link_pattern, html_content, re.IGNORECASE)
            return len(external_booking_links)
        except Exception:
            return 0

    async def analyze_page(self, page, url, timeout):
        """Analyze a single page for chatbots, booking tech, and OTA dependencies."""
        try:
            await page.goto(url, timeout=timeout, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)  # Wait for dynamic content
            
            # Get page content
            html_content = await page.content()
            text_content = await page.evaluate('document.body.innerText || ""')
            
            # Analyze the content
            analysis = self.analyze_page_content(html_content, text_content, url)
            
            # Enhanced dynamic chatbot detection
            await self._detect_dynamic_elements(page, analysis)
            
            # Check for network requests that might indicate chat/booking services
            network_analysis = await self._analyze_network_requests(page)
            analysis['chatbot_types'].extend(network_analysis['chatbot_services'])
            analysis['booking_technology'].extend(network_analysis['booking_services'])
            analysis['ota_dependencies'].extend(network_analysis['ota_services'])
            
            if network_analysis['chatbot_services']:
                analysis['has_chatbot'] = True
            
            return analysis
            
        except Exception as e:
            logger.warning(f"    Error analyzing {url}: {e}")
            return {
                'has_chatbot': None,
                'chatbot_types': [],
                'booking_technology': [],
                'ota_dependencies': [],
                'analysis_details': {},
                'error': str(e)
            }

    async def _detect_dynamic_elements(self, page, analysis):
        """Detect chatbots and widgets that load dynamically."""
        try:
            # Wait a bit more for dynamic content
            await page.wait_for_timeout(2000)
            
            # Enhanced chatbot selectors (more comprehensive)
            chatbot_selectors = [
                # Generic chat elements
                '[class*="chat"]:not([class*="chart"])', '[id*="chat"]:not([id*="chart"])',
                '[class*="widget"]', '[id*="widget"]',
                '[class*="messenger"]', '[id*="messenger"]',
                
                # Common chat UI patterns
                '[class*="bubble"]', '[class*="float"]',
                '[aria-label*="chat" i]', '[title*="chat" i]',
                '[data-testid*="chat"]', '[role="dialog"][aria-label*="chat" i]',
                
                # Iframe patterns
                'iframe[src*="chat"]', 'iframe[src*="widget"]',
                'iframe[src*="support"]', 'iframe[src*="help"]',
                
                # Button patterns
                'button[class*="chat"]', 'button[id*="chat"]',
                'a[href*="chat"]', 'div[onclick*="chat"]'
            ]
            
            for selector in chatbot_selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    for element in elements[:3]:  # Check first 3 elements
                        try:
                            # Get element properties
                            element_html = await element.inner_html()
                            element_text = await element.inner_text()
                            element_attrs = await element.evaluate('el => Array.from(el.attributes).map(attr => attr.name + "=" + attr.value)')
                            
                            # Check if element is visible (not hidden chat widgets)
                            is_visible = await element.is_visible()
                            
                            # Analyze element content
                            content_to_check = (element_html + " " + element_text + " " + str(element_attrs)).lower()
                            
                            # Chat-specific keywords in element
                            chat_keywords = [
                                'start chat', 'chat with', 'live chat', 'support chat',
                                'help chat', 'message us', 'ask question', 'need help',
                                'contact support', 'chat now', 'talk to us'
                            ]
                            
                            has_chat_keywords = any(keyword in content_to_check for keyword in chat_keywords)
                            
                            # Widget-like behavior (positioned fixed, high z-index)
                            widget_style = await element.evaluate('''
                                el => {
                                    const style = window.getComputedStyle(el);
                                    return {
                                        position: style.position,
                                        zIndex: style.zIndex,
                                        bottom: style.bottom,
                                        right: style.right
                                    };
                                }
                            ''')
                            
                            is_widget_positioned = (
                                widget_style.get('position') == 'fixed' and
                                (widget_style.get('bottom') != 'auto' or widget_style.get('right') != 'auto')
                            )
                            
                            # If it looks like a chat widget
                            if (has_chat_keywords or is_widget_positioned) and is_visible:
                                analysis['has_chatbot'] = True
                                analysis['chatbot_types'].append('dynamic_chat_widget')
                                break
                                
                        except Exception as e:
                            continue
            
            # Check for booking elements
            booking_selectors = [
                '[class*="book"]', '[id*="book"]',
                '[class*="reserv"]', '[id*="reserv"]',
                '[class*="ticket"]', '[id*="ticket"]',
                'form[action*="book"]', 'form[action*="reserv"]'
            ]
            
            for selector in booking_selectors:
                elements = await page.query_selector_all(selector)
                if len(elements) >= 2:  # Multiple booking elements suggest booking system
                    analysis['booking_technology'].append('dynamic_booking_system')
                    break
                    
        except Exception as e:
            logger.warning(f"Error in dynamic element detection: {e}")

    async def _analyze_network_requests(self, page):
        """Analyze network requests to detect third-party services."""
        network_analysis = {
            'chatbot_services': [],
            'booking_services': [],
            'ota_services': []
        }
        
        try:
            # Get all network requests made by the page
            network_requests = await page.evaluate('''
                () => {
                    const requests = [];
                    if (window.performance && window.performance.getEntriesByType) {
                        const entries = window.performance.getEntriesByType('resource');
                        for (const entry of entries) {
                            if (entry.name.includes('http')) {
                                requests.push(entry.name);
                            }
                        }
                    }
                    return requests;
                }
            ''')
            
            # Analyze each request URL
            for request_url in network_requests:
                url_lower = request_url.lower()
                
                # Check for chatbot services in network requests
                chatbot_domains = [
                    'chat', 'support', 'widget', 'messenger', 'livechat',
                    'helpdesk', 'zendesk', 'intercom', 'drift', 'crisp',
                    'tidio', 'tawk', 'olark', 'freshchat', 'purechat'
                ]
                
                if any(domain in url_lower for domain in chatbot_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['chatbot_services']]:
                        network_analysis['chatbot_services'].append(f'network_{domain}')
                
                # Check for booking services
                booking_domains = [
                    'booking', 'reserv', 'ticket', 'payment', 'checkout',
                    'stripe', 'paypal', 'square', 'calendar'
                ]
                
                if any(domain in url_lower for domain in booking_domains):
                    domain = urlparse(request_url).netloc
                    if domain not in [item.split('_')[0] for item in network_analysis['booking_services']]:
                        network_analysis['booking_services'].append(f'network_{domain}')
                
                # Check for OTA services
                ota_domains = [
                    'getyourguide', 'viator', 'tripadvisor', 'expedia',
                    'booking.com', 'klook', 'tiqets', 'headout'
                ]
                
                if any(domain in url_lower for domain in ota_domains):
                    domain = urlparse(request_url).netloc
                    network_analysis['ota_services'].append(f'network_{domain}')
            
        except Exception as e:
            logger.warning(f"Error analyzing network requests: {e}")
        
        return network_analysis

    async def analyze_website(self, browser, url, timeout):
        """Analyze a website across multiple pages."""
        all_results = {
            'has_chatbot': False,
            'chatbot_types': set(),
            'booking_technology': set(),
            'ota_dependencies': set(),
            'analysis_details': {},
            'pages_analyzed': 0
        }
        
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        )
        page = await context.new_page()
        
        try:
            domain = urlparse(url).netloc
            visited_urls = {url.rstrip('/')}
            
            # Analyze main page
            main_analysis = await self.analyze_page(page, url, timeout)
            if 'error' not in main_analysis:
                all_results['has_chatbot'] = main_analysis['has_chatbot']
                all_results['chatbot_types'].update(main_analysis['chatbot_types'])
                all_results['booking_technology'].update(main_analysis['booking_technology'])
                all_results['ota_dependencies'].update(main_analysis['ota_dependencies'])
                all_results['analysis_details'] = main_analysis['analysis_details']
                all_results['pages_analyzed'] += 1
            
            # Find important pages to analyze
            important_pages = ['/booking', '/book', '/tours', '/experiences', '/reserve', '/contact']
            links_to_check = []
            
            try:
                # Get navigation links
                nav_links = await page.query_selector_all('nav a, .navigation a, #menu a, .menu a')
                for link in nav_links[:10]:  # Limit to first 10 nav links
                    href = await link.get_attribute('href')
                    if href:
                        full_url = urljoin(url, href.strip()).rstrip('/')
                        if full_url.startswith('http') and urlparse(full_url).netloc == domain:
                            links_to_check.append(full_url)
                
                # Add important pages if they exist
                for important_page in important_pages:
                    potential_url = urljoin(url, important_page).rstrip('/')
                    links_to_check.append(potential_url)
                
            except Exception as e:
                logger.warning(f"Error getting navigation links from {url}: {e}")
            
            # Analyze additional pages
            pages_checked = 1
            for link_url in set(links_to_check):
                if pages_checked >= self.max_pages_per_site or link_url in visited_urls:
                    continue
                
                try:
                    page_analysis = await self.analyze_page(page, link_url, timeout)
                    if 'error' not in page_analysis:
                        if page_analysis['has_chatbot']:
                            all_results['has_chatbot'] = True
                        all_results['chatbot_types'].update(page_analysis['chatbot_types'])
                        all_results['booking_technology'].update(page_analysis['booking_technology'])
                        all_results['ota_dependencies'].update(page_analysis['ota_dependencies'])
                        all_results['pages_analyzed'] += 1
                    
                    visited_urls.add(link_url)
                    pages_checked += 1
                    await asyncio.sleep(self.delay)
                    
                except Exception as e:
                    logger.warning(f"Error analyzing additional page {link_url}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"  Error processing {url}: {e}")
            all_results['error'] = str(e)
        finally:
            await context.close()
        
        # Convert sets to lists for JSON serialization
        return {
            'has_chatbot': all_results['has_chatbot'],
            'chatbot_types': list(all_results['chatbot_types']),
            'booking_technology': list(all_results['booking_technology']),
            'ota_dependencies': list(all_results['ota_dependencies']),
            'analysis_details': all_results['analysis_details'],
            'pages_analyzed': all_results['pages_analyzed']
        }

    async def process_csv(self, input_csv, output_csv, batch_size, mode, concurrency_override, timeout_override):
        """Process CSV file to analyze websites."""
        try:
            df = pd.read_csv(input_csv)
            
            # Override default parameters if provided
            concurrency_limit = concurrency_override if concurrency_override is not None else self.concurrency
            timeout = timeout_override * 1000 if timeout_override is not None else self.timeout

            # Add new columns if they don't exist
            new_columns = ['has_chatbot', 'chatbot_types', 'booking_technology', 'ota_dependencies', 'analysis_summary', 'pages_analyzed']
            for col in new_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            
            # Prepare batch based on mode
            if mode == 'retry':
                print("\n🔍 Finding companies that previously failed...")
                df_todo = df[df['has_chatbot'].str.contains("Error|No analysis", na=False) | df['has_chatbot'].isna()].copy()
                if not df_todo.empty:
                    print(f"Found {len(df_todo)} failed/incomplete rows to retry.")
                    for col in new_columns:
                        df.loc[df_todo.index, col] = pd.NA
            else:
                df_todo = df[df['has_chatbot'].isna()].copy()

            if df_todo.empty:
                print("\n🎉 No companies to process for the selected mode. All done!")
                return

            df_batch = df_todo.head(batch_size)
            print(f"\nProcessing the next batch of {len(df_batch)} companies.")

            # Find URL column
            url_columns = ['Website URL', 'website url', 'url', 'domain', 'website', 'company_url', 'site', 'Website', 'URL', 'Domain']
            url_column = next((col for col in url_columns if col in df.columns), df.columns[0])
            
            # Run parallel analysis
            semaphore = asyncio.Semaphore(concurrency_limit)
            
            async def analyze_with_semaphore(browser, index, company_name, url):
                async with semaphore:
                    logger.info(f"-> Starting analysis for index {index}: {company_name}")
                    try:
                        analysis = await self.analyze_website(browser, url, timeout)
                        
                        # Update dataframe with results
                        df.loc[index, 'has_chatbot'] = 'Yes' if analysis['has_chatbot'] else 'No'
                        df.loc[index, 'chatbot_types'] = '; '.join(analysis['chatbot_types']) if analysis['chatbot_types'] else 'None detected'
                        df.loc[index, 'booking_technology'] = '; '.join(analysis['booking_technology']) if analysis['booking_technology'] else 'None detected'
                        df.loc[index, 'ota_dependencies'] = '; '.join(analysis['ota_dependencies']) if analysis['ota_dependencies'] else 'None detected'
                        df.loc[index, 'pages_analyzed'] = analysis.get('pages_analyzed', 0)
                        
                        # Create summary
                        summary_parts = []
                        if analysis['has_chatbot']:
                            summary_parts.append("Has chatbot")
                        if analysis['booking_technology']:
                            summary_parts.append(f"Booking: {', '.join(analysis['booking_technology'][:2])}")
                        if analysis['ota_dependencies']:
                            summary_parts.append(f"OTA: {', '.join(analysis['ota_dependencies'][:2])}")
                        
                        df.loc[index, 'analysis_summary'] = '; '.join(summary_parts) if summary_parts else 'No key features detected'
                        
                        logger.info(f"<- Finished analysis for index {index}: {company_name}")
                        
                    except Exception as e:
                        error_msg = f"Error: {str(e)[:100]}"
                        for col in new_columns[:-1]:  # All except pages_analyzed
                            df.loc[index, col] = error_msg
                        df.loc[index, 'pages_analyzed'] = 0
                        logger.error(f"<- Analysis failed for index {index}: {e}")

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                tasks = []
                for index, row in df_batch.iterrows():
                    clean_url = self.clean_url(row.get(url_column))
                    if clean_url:
                        company_name = row.get('Company Name', f'Company at index {index}')
                        tasks.append(analyze_with_semaphore(browser, index, company_name, clean_url))
                    else:
                        df.loc[index, 'has_chatbot'] = 'Invalid URL'
                        df.loc[index, 'analysis_summary'] = 'Invalid URL provided'
                
                print(f"🚀 Running {len(tasks)} analysis jobs in parallel (limit: {concurrency_limit}, timeout: {timeout/1000}s)...")
                await asyncio.gather(*tasks)
                await browser.close()
            
            # Save results
            df.to_csv(output_csv, index=False)
            print(f"\n✅ Batch complete. Results saved to {output_csv}")
            
            # Progress report
            total_companies = len(df)
            companies_completed = total_companies - len(df[df['has_chatbot'].isna()])
            companies_with_chatbots = len(df[df['has_chatbot'] == 'Yes'])
            companies_without_chatbots = len(df[df['has_chatbot'] == 'No'])
            
            print("\n" + "="*60)
            print("ANALYSIS REPORT:")
            print(f"Total companies: {total_companies}")
            print(f"Companies analyzed: {companies_completed}")
            print(f"Companies WITH chatbots: {companies_with_chatbots}")
            print(f"Companies WITHOUT chatbots: {companies_without_chatbots}")
            if total_companies > 0:
                print(f"Completion: {companies_completed/total_companies*100:.1f}%")
                if companies_without_chatbots > 0:
                    print(f"🎯 Potential prospects (no chatbot): {companies_without_chatbots}")
            print("="*60)
            
        except Exception as e:
            print(f"An error occurred during CSV processing: {e}")

    def clean_url(self, url):
        """Clean and validate a URL."""
        if not url or pd.isna(url):
            return None
        url = str(url).strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url.rstrip('/')

def get_user_input(prompt, default, is_int=False):
    """Helper function to get validated user input."""
    while True:
        val = input(prompt).strip() or default
        if not is_int:
            return val
        try:
            int_val = int(val)
            if int_val > 0:
                return int_val
            else:
                print("Please enter a positive number.")
        except ValueError:
            print("Invalid input. Please enter a whole number.")

def main():
    """Main function to run the tour operator analyzer."""
    print("🎯 Tour Operator Website Analyzer 🎯")
    print("Detecting: Chatbots | Booking Technology | OTA Dependencies")
    print("=" * 70)
    
    input_csv = get_user_input("Enter input CSV filename (default: companies.csv): ", 'companies.csv')
    output_csv = get_user_input("Enter output CSV filename (default: analysis_results.csv): ", 'analysis_results.csv')
    
    mode_choice = get_user_input("Choose mode: [1] Analyze new (default), [2] Retry failed: ", '1')
    mode = 'retry' if mode_choice == '2' else 'process'
    
    batch_size = get_user_input("How many companies to analyze in this batch? (default: 50): ", '50', is_int=True)
    
    concurrency_override = None
    timeout_override = None
    
    if mode == 'retry':
        print("\n--- RETRY MODE SETTINGS ---")
        concurrency_override = get_user_input(f"Set concurrency limit (default: 10): ", '10', is_int=True)
        timeout_override = get_user_input("Set timeout in seconds (default: 90): ", '90', is_int=True)
    
    analyzer = TourOperatorAnalyzer()
    asyncio.run(analyzer.process_csv(input_csv, output_csv, batch_size, mode, concurrency_override, timeout_override))
    
    print(f"\nDone! Check '{output_csv}' for results.")
    print("💡 TIP: Filter for companies with 'has_chatbot' = 'No' to find prospects!")

if __name__ == "__main__":
    main()